// Demo/smoke-test against a REAL, reachable hsm-core-service -- unlike a
// pure-local-crypto reference, these classes have no local crypto to
// self-test in isolation; every call here is a real HTTP round trip.
// Configure via env vars so this can point at any environment.
//
// No args: dotnet run
//   Runs HsmCoreBatchFile's own self-test (Tier 1 -- encrypt+decrypt a demo
//   file directly against hsm-core-service, no hsm-bulk-service involved
//   at any point).
//
// Two args: dotnet run -- <bulk-produced-file> <target-file>
//   Runs HsmBulkFileReader against a REAL file produced by hsm-bulk-client's
//   FileBulkJob (Tier 3) -- decrypts it purely via hsm-core-service,
//   hsm-bulk-service never contacted on this side.

using System;
using System.IO;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Hsm.BulkClient.Examples;

string baseUrl = Environment.GetEnvironmentVariable("HSM_CORE_BASE_URL") ?? "http://localhost:3105";
string apiV1Prefix = Environment.GetEnvironmentVariable("HSM_CORE_API_V1_PREFIX") ?? "/api/sensec/hsm/v1";
string appId = Environment.GetEnvironmentVariable("HSM_CORE_APP_ID") ?? "payments-svc";
string token = Environment.GetEnvironmentVariable("HSM_CORE_TOKEN") ?? "demo-token-payments-svc";

var client = new HsmCoreClient(baseUrl, apiV1Prefix, appId, token);

if (args.Length == 2)
{
    await HsmBulkFileReader.DecryptBulkFileAsync(client, args[0], args[1]);
    Console.WriteLine($"Decrypted {args[0]} -> {args[1]} via hsm-core-service directly (hsm-bulk-service never contacted).");
    return;
}

byte[] demoPlaintext = new byte[37_000]; // forces multiple chunks at the small chunk size below
RandomNumberGenerator.Fill(demoPlaintext);

string tmp = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
Directory.CreateDirectory(tmp);
try
{
    string src = Path.Combine(tmp, "plain.bin");
    string manifestPath = Path.Combine(tmp, "manifest.json");
    string dec = Path.Combine(tmp, "decrypted.bin");
    File.WriteAllBytes(src, demoPlaintext);

    FileManifest manifest = await HsmCoreBatchFile.EncryptFileAsync(client, src, manifestPath, chunkSizeBytes: 4096);
    Console.WriteLine($"encrypted: chunk_count={manifest.ChunkCount} plaintext_sha256={manifest.PlaintextSha256}");

    await HsmCoreBatchFile.DecryptFileAsync(client, manifestPath, dec);
    string actualSha256 = Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(dec))).ToLowerInvariant();
    bool match = actualSha256 == manifest.PlaintextSha256;
    Console.WriteLine($"decrypted: match={match}");
    if (!match) throw new InvalidOperationException("round-trip mismatch");
    Console.WriteLine("OK -- round trip verified against a real hsm-core-service");
}
finally
{
    Directory.Delete(tmp, recursive: true);
}
