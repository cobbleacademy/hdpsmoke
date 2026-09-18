// Bearer-token providers for HsmCoreClient (HsmCoreBatchFile.cs). Covers 3 of
// hsm-crypto-client's 4 SvcConfig.AuthMode values -- STATIC, SELF_SIGNED_JWT,
// AZURE_AD. MTLS is deliberately not ported here: it authenticates at the TLS
// transport layer (a client cert/key on the connection itself), not via a
// bearer token, so it doesn't fit ITokenProvider's shape at all -- HttpClient
// would need its own HttpClientHandler.ClientCertificates wired through
// HsmCoreClient separately if that's ever needed.
//
// SelfSignedJwtTokenProvider and AzureAdTokenProvider are direct ports of
// hsm-crypto-client's own SelfSignedJwtTokenProvider.java / AzureAdTokenProvider.java
// (com.hsm.client.svc) -- same claims shape, same TTL, same credential
// cascade. If the two ever disagree, that's a bug in one of them, not an
// intentional difference. StaticTokenProvider mirrors examples/python's
// auth.py, this directory's own sibling reference.
//
// RS256 is hand-rolled here (System.Security.Cryptography.RSA, not a JWT
// NuGet package) -- same call examples/python's auth.py made (hand-rolled,
// not PyJWT): keeps this directory's dependency footprint at just
// Azure.Identity (AZURE_AD only), consistent with HsmCoreBatchFile.cs's own
// "no external NuGet needed" stance for everything else. RS256 (RSASSA-PKCS1-v1_5
// + SHA-256) is simple enough that hand-rolling it isn't a real risk the way
// hand-rolling AES-GCM would be.
//
// Dependencies, only for the mode you actually use:
//   STATIC          -- none (BCL only)
//   SELF_SIGNED_JWT -- none (BCL only -- System.Security.Cryptography.RSA)
//   AZURE_AD        -- NuGet: Azure.Identity

using System;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;

namespace Hsm.BulkClient.Examples
{
    /// <summary>
    /// Supplies the bearer token HsmCoreClient sends on every request. Called
    /// fresh before each call, not cached by HsmCoreClient itself --
    /// implementations decide their own caching (see SelfSignedJwtTokenProvider,
    /// AzureAdTokenProvider).
    /// </summary>
    public interface ITokenProvider
    {
        string GetBearerToken();
    }

    /// <summary>
    /// A fixed bearer token, sent as-is on every call. Fine for demo/mock-mode
    /// tokens (never expire); a real Azure AD JWT here would expire mid-session --
    /// same caveat hsm-crypto-client's HsmCryptoClient.Builder.staticToken documents.
    /// </summary>
    public sealed class StaticTokenProvider : ITokenProvider
    {
        private readonly string _token;

        public StaticTokenProvider(string token)
        {
            _token = token;
        }

        public string GetBearerToken() => _token;
    }

    /// <summary>
    /// Locally signs a short-lived RS256 bearer assertion (RFC 7523-style) with
    /// this app's own private key instead of acquiring a token from an external
    /// IdP -- port of hsm-crypto-client's SelfSignedJwtTokenProvider.java.
    /// "Renewal" is pure local computation, never a network call.
    ///
    /// Matches hsm-core-service's SelfSignedAppKeyJwtValidator exactly (confirmed
    /// directly against that source, not assumed): RS256 algorithm, sub/iss set
    /// to appId, aud must intersect the server's configured hsm.jwt.audience
    /// list (default "hsm-core-service"), iat/exp required, token lifetime
    /// capped well under the server's 5-minute MAX_TTL. The server does NOT
    /// check iss or track jti for replay protection (a known, documented gap on
    /// the server side, not something this client needs to compensate for) --
    /// both are still set here for parity with the JVM provider and in case
    /// that changes later.
    ///
    /// Caches the signed token, re-signing only within RefreshMargin of expiry
    /// -- signing is cheap, but no reason to re-sign on every single call in a
    /// tight loop. Thread-safe via a simple lock, same reasoning as the Java
    /// provider's ReentrantLock (multiple workers may share one instance).
    /// </summary>
    public sealed class SelfSignedJwtTokenProvider : ITokenProvider
    {
        private static readonly TimeSpan TokenTtl = TimeSpan.FromMinutes(2); // well under SelfSignedAppKeyJwtValidator's server-side 5-minute MAX_TTL
        private static readonly TimeSpan RefreshMargin = TimeSpan.FromSeconds(15);

        private readonly RSA _signingKey;
        private readonly string _appId;
        private readonly string _audience;
        private readonly object _gate = new();

        private string? _cachedToken;
        private DateTimeOffset _cachedExpiry = DateTimeOffset.MinValue;

        public SelfSignedJwtTokenProvider(RSA signingKey, string appId, string audience = "hsm-core-service")
        {
            _signingKey = signingKey;
            _appId = appId;
            _audience = audience;
        }

        public string GetBearerToken()
        {
            DateTimeOffset now = DateTimeOffset.UtcNow;
            if (_cachedToken != null && now < _cachedExpiry - RefreshMargin)
                return _cachedToken;

            lock (_gate)
            {
                if (_cachedToken != null && now < _cachedExpiry - RefreshMargin)
                    return _cachedToken;
                return MintToken(now);
            }
        }

        private string MintToken(DateTimeOffset now)
        {
            DateTimeOffset expiry = now.Add(TokenTtl);
            string headerB64 = Base64UrlJson(new { alg = "RS256", typ = "JWT" });
            string claimsB64 = Base64UrlJson(new
            {
                sub = _appId,
                iss = _appId,
                aud = _audience,
                iat = now.ToUnixTimeSeconds(),
                exp = expiry.ToUnixTimeSeconds(),
                jti = Guid.NewGuid().ToString(),
            });
            string signingInput = $"{headerB64}.{claimsB64}";
            byte[] signature = _signingKey.SignData(
                Encoding.ASCII.GetBytes(signingInput), HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            string token = $"{signingInput}.{Base64Url(signature)}";

            _cachedToken = token;
            _cachedExpiry = expiry;
            return token;
        }

        private static string Base64Url(byte[] data) =>
            Convert.ToBase64String(data).TrimEnd('=').Replace('+', '-').Replace('/', '_');

        private static string Base64UrlJson(object obj) =>
            Base64Url(JsonSerializer.SerializeToUtf8Bytes(obj));
    }

    /// <summary>
    /// Acquires a real Azure AD access token, scoped to azureTokenScope --
    /// port of hsm-crypto-client's AzureAdTokenProvider.java, same credential
    /// cascade (not Azure.Identity.DefaultAzureCredential, deliberately --
    /// see that file's own comment: DefaultAzureCredential's chain includes
    /// IntelliJCredential/VisualStudioCodeCredential, neither relevant off an
    /// IDE, and VisualStudioCodeCredential throws a hard
    /// CredentialUnavailableException rather than silently skipping when its
    /// optional broker package isn't referenced):
    ///
    ///   1. WorkloadIdentityCredential  -- AZURE_CLIENT_ID + AZURE_TENANT_ID +
    ///      a federated token file (AZURE_FEDERATED_TOKEN_FILE, default
    ///      /var/run/secrets/azure/tokens/azure-identity-token) -- the common
    ///      in-cluster case, no client secret needed at all.
    ///   2. ClientSecretCredential  -- only when AZURE_CLIENT_SECRET is also
    ///      set, for callers off Azure-hosted compute entirely (e.g. local
    ///      verification) where neither Workload Identity nor IMDS is reachable.
    ///   3. ManagedIdentityCredential(clientId)  -- AZURE_CLIENT_ID set, no
    ///      secret -- a user-assigned identity.
    ///   4. ChainedTokenCredential(AzureCliCredential, AzurePowerShellCredential,
    ///      ManagedIdentityCredential)  -- local dev fallbacks, then
    ///      system-assigned Managed Identity.
    ///
    /// GetToken already caches internally and only performs the real
    /// federated-token exchange when the cached token is expired or
    /// near-expiry -- calling it before every request costs nothing extra on
    /// the common (cache-hit) path.
    /// </summary>
    public sealed class AzureAdTokenProvider : ITokenProvider
    {
        private const string DefaultTokenFile = "/var/run/secrets/azure/tokens/azure-identity-token";

        private readonly Azure.Core.TokenCredential _credential;
        private readonly Azure.Core.TokenRequestContext _context;

        public AzureAdTokenProvider(string azureTokenScope, Azure.Core.TokenCredential? credential = null)
        {
            _credential = credential ?? BuildCredential();
            _context = new Azure.Core.TokenRequestContext(new[] { azureTokenScope });
        }

        public string GetBearerToken() => _credential.GetToken(_context, default).Token;

        private static Azure.Core.TokenCredential BuildCredential()
        {
            string tokenFile = Environment.GetEnvironmentVariable("AZURE_FEDERATED_TOKEN_FILE") ?? DefaultTokenFile;
            string? clientId = Environment.GetEnvironmentVariable("AZURE_CLIENT_ID");
            string? tenantId = Environment.GetEnvironmentVariable("AZURE_TENANT_ID");

            if (!string.IsNullOrWhiteSpace(clientId) && !string.IsNullOrWhiteSpace(tenantId) && System.IO.File.Exists(tokenFile))
            {
                return new Azure.Identity.WorkloadIdentityCredential(new Azure.Identity.WorkloadIdentityCredentialOptions
                {
                    TenantId = tenantId,
                    ClientId = clientId,
                    TokenFilePath = tokenFile,
                });
            }

            // Explicit App Registration secret, only when configured -- checked before the
            // Managed Identity/IMDS fallback below since a caller who set a client secret
            // clearly intends to use it, not fall through to node-identity resolution (which
            // also fails outright off Azure-hosted compute, e.g. a local dev machine). Same
            // ordering as AzureAdTokenProvider.java.
            string? clientSecret = Environment.GetEnvironmentVariable("AZURE_CLIENT_SECRET");
            if (!string.IsNullOrWhiteSpace(clientSecret) && !string.IsNullOrWhiteSpace(clientId) && !string.IsNullOrWhiteSpace(tenantId))
            {
                return new Azure.Identity.ClientSecretCredential(tenantId, clientId, clientSecret);
            }

            if (!string.IsNullOrWhiteSpace(clientId))
            {
                return new Azure.Identity.ManagedIdentityCredential(clientId);
            }

            return new Azure.Identity.ChainedTokenCredential(
                new Azure.Identity.AzureCliCredential(),
                new Azure.Identity.AzurePowerShellCredential(),
                new Azure.Identity.ManagedIdentityCredential());
        }
    }

    /// <summary>
    /// Selects and constructs an ITokenProvider from HSM_CORE_* env vars, for
    /// Program.cs's own demo/self-test -- mirrors examples/python's
    /// build_token_provider_from_env, adapted to this directory's own
    /// HSM_CORE_* naming. Not required to use these providers directly --
    /// construct one yourself for anything this doesn't cover (e.g. a
    /// credential object your hosting platform already built for you, as in
    /// an Azure Function using its own bound managed identity).
    ///
    /// HSM_CORE_AUTH_MODE selects the mode -- STATIC (default), SELF_SIGNED_JWT,
    /// or AZURE_AD. MTLS is not supported here -- see this file's own header
    /// comment for why.
    /// </summary>
    public static class TokenProviderFactory
    {
        public static ITokenProvider BuildFromEnv(string appId)
        {
            string authMode = (Environment.GetEnvironmentVariable("HSM_CORE_AUTH_MODE") ?? "STATIC").Trim().ToUpperInvariant();

            switch (authMode)
            {
                case "STATIC":
                    return new StaticTokenProvider(
                        Environment.GetEnvironmentVariable("HSM_CORE_TOKEN") ?? "demo-token-payments-svc");

                case "SELF_SIGNED_JWT":
                {
                    string? pemPath = Environment.GetEnvironmentVariable("HSM_CORE_SIGNING_PRIVATE_KEY_PEM_PATH");
                    string? pemInline = Environment.GetEnvironmentVariable("HSM_CORE_SIGNING_PRIVATE_KEY_PEM");
                    string pem = pemPath != null
                        ? System.IO.File.ReadAllText(pemPath)
                        : pemInline ?? throw new InvalidOperationException(
                            "HSM_CORE_AUTH_MODE=SELF_SIGNED_JWT requires either " +
                            "HSM_CORE_SIGNING_PRIVATE_KEY_PEM_PATH or HSM_CORE_SIGNING_PRIVATE_KEY_PEM");

                    RSA signingKey = RSA.Create();
                    signingKey.ImportFromPem(pem);
                    string audience = Environment.GetEnvironmentVariable("HSM_CORE_SELF_SIGNED_AUDIENCE") ?? "hsm-core-service";
                    return new SelfSignedJwtTokenProvider(signingKey, appId, audience);
                }

                case "AZURE_AD":
                {
                    string scope = Environment.GetEnvironmentVariable("HSM_CORE_AZURE_TOKEN_SCOPE")
                        ?? throw new InvalidOperationException("HSM_CORE_AUTH_MODE=AZURE_AD requires HSM_CORE_AZURE_TOKEN_SCOPE");
                    return new AzureAdTokenProvider(scope);
                }

                default:
                    throw new InvalidOperationException(
                        $"HSM_CORE_AUTH_MODE must be STATIC, SELF_SIGNED_JWT, or AZURE_AD, got '{authMode}'. " +
                        "MTLS isn't supported by this module -- see Auth.cs's own header comment.");
            }
        }
    }
}
