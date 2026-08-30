package com.hsm.core.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hsm.core.audit.AuditLogger;
import com.hsm.core.auth.AppRegistryException;
import com.hsm.core.auth.AppRegistryService;
import com.hsm.core.web.AuthenticatedCaller;
import com.hsm.core.web.ClientIpResolver;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/**
 * Fourth, optional authentication mechanism alongside JwtAppIdAuthenticationFilter's
 * three (STATIC/AZURE_AD/SELF_SIGNED_JWT) -- accepts a mutual-TLS client certificate
 * in place of a bearer token. Runs ahead of JwtAppIdAuthenticationFilter in the
 * chain (see SecurityConfig): if a client certificate was presented and it
 * authenticates successfully, this sets the SecurityContext and
 * JwtAppIdAuthenticationFilter's own added early-exit guard (skip if already
 * authenticated) lets the request through without ever needing a bearer token. If
 * no certificate was presented at all, this filter is a no-op and the request
 * falls through to the JWT filter exactly as before -- mTLS is opt-in per request,
 * not a replacement that breaks non-mTLS callers.
 *
 * <p>Only registered as a bean when hsm.security.mtls-enabled=true (see
 * MtlsServerConfig) -- when disabled, this class doesn't exist in the filter
 * chain at all, so its behavior can't matter even in theory.
 *
 * <p><b>Identity resolution is fingerprint-pinned, not chain-of-trust.</b> The
 * embedded Tomcat connector (MtlsServerConfig) is configured to accept any
 * client certificate at the TLS handshake itself -- self-signed certs have no
 * CA to validate a chain against, so real validation happens here instead:
 * the caller's claimed X-App-ID header names which app's registered
 * fingerprint (AppRegistryService.getMtlsCertFingerprint) to compare the
 * actually-presented certificate's SHA-256 fingerprint against. A certificate
 * that doesn't match a registered fingerprint for the claimed app_id is
 * rejected outright here, not silently passed through to the JWT filter --
 * presenting a certificate is an explicit choice to authenticate via mTLS,
 * so a bad one fails loudly the same way a bad JWT signature does.
 */
public class MtlsAppIdAuthenticationFilter extends OncePerRequestFilter {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AntPathMatcher PATH_MATCHER = new AntPathMatcher();
    private static final String CERT_ATTRIBUTE = "jakarta.servlet.request.X509Certificate";

    private final AppRegistryService appRegistry;
    private final AuditLogger auditLogger;
    private final HsmSecurityProperties securityProperties;
    private final String apiV1Prefix;

    public MtlsAppIdAuthenticationFilter(
            AppRegistryService appRegistry,
            AuditLogger auditLogger,
            HsmSecurityProperties securityProperties,
            String apiV1Prefix
    ) {
        this.appRegistry = appRegistry;
        this.auditLogger = auditLogger;
        this.securityProperties = securityProperties;
        this.apiV1Prefix = apiV1Prefix;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getRequestURI();
        String method = request.getMethod();
        for (HsmSecurityProperties.AccessRule rule : securityProperties.accessRules()) {
            String pattern = apiV1Prefix + rule.pattern();
            if (rule.methods().contains(method) && PATH_MATCHER.match(pattern, path)) {
                return false; // protected -- do not skip
            }
        }
        return true; // not a configured resource -- no auth required
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        X509Certificate[] certs = (X509Certificate[]) request.getAttribute(CERT_ATTRIBUTE);
        if (certs == null || certs.length == 0) {
            // No certificate presented -- not an mTLS attempt at all, let the JWT filter handle it.
            filterChain.doFilter(request, response);
            return;
        }

        String callerIp = ClientIpResolver.resolve(request);
        String xAppId = request.getHeader("X-App-ID");
        if (xAppId == null || xAppId.isBlank()) {
            auditLogger.log("auth_failure", "caller_ip", callerIp,
                    "status", "failure", "reason", "mtls_missing_x_app_id_header");
            writeError(response, HttpServletResponse.SC_UNAUTHORIZED, "X-App-ID header required for mTLS authentication");
            return;
        }

        String presentedFingerprint;
        try {
            presentedFingerprint = fingerprint(certs[0]);
        } catch (CertificateEncodingException | NoSuchAlgorithmException e) {
            auditLogger.log("auth_failure", "app_id", xAppId, "caller_ip", callerIp,
                    "status", "failure", "reason", "mtls_cert_unreadable: " + e.getMessage());
            writeError(response, HttpServletResponse.SC_UNAUTHORIZED, "Client certificate could not be read");
            return;
        }

        String registeredFingerprint;
        try {
            registeredFingerprint = appRegistry.getMtlsCertFingerprint(xAppId);
        } catch (AppRegistryException e) {
            auditLogger.log("auth_failure", "app_id", xAppId, "caller_ip", callerIp,
                    "status", "failure", "reason", "unknown_or_inactive_app: " + e.getMessage());
            writeError(response, HttpServletResponse.SC_FORBIDDEN, e.getMessage());
            return;
        }

        if (registeredFingerprint == null) {
            auditLogger.log("auth_failure", "app_id", xAppId, "caller_ip", callerIp,
                    "status", "failure", "reason", "mtls_no_cert_registered");
            writeError(response, HttpServletResponse.SC_UNAUTHORIZED, "app_id has no registered mTLS certificate");
            return;
        }

        if (!registeredFingerprint.equalsIgnoreCase(presentedFingerprint)) {
            auditLogger.log("auth_failure", "app_id", xAppId, "caller_ip", callerIp,
                    "status", "failure", "reason", "mtls_fingerprint_mismatch");
            writeError(response, HttpServletResponse.SC_UNAUTHORIZED, "Client certificate not recognized for this app_id");
            return;
        }

        List<String> scopes;
        try {
            scopes = appRegistry.getScopes(xAppId);
        } catch (AppRegistryException e) {
            auditLogger.log("auth_failure", "app_id", xAppId, "caller_ip", callerIp,
                    "status", "failure", "reason", "unknown_or_inactive_app: " + e.getMessage());
            writeError(response, HttpServletResponse.SC_FORBIDDEN, e.getMessage());
            return;
        }

        String sub = certs[0].getSubjectX500Principal().getName();
        AuthenticatedCaller caller = new AuthenticatedCaller(xAppId, sub, new ArrayList<>(scopes));

        List<GrantedAuthority> authorities = new ArrayList<>();
        for (String scope : scopes) {
            authorities.add(new SimpleGrantedAuthority(scope));
        }
        SecurityContextHolder.getContext().setAuthentication(
                new UsernamePasswordAuthenticationToken(caller, null, authorities));

        filterChain.doFilter(request, response);
    }

    private static String fingerprint(X509Certificate cert) throws CertificateEncodingException, NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        return HexFormat.of().formatHex(digest.digest(cert.getEncoded()));
    }

    private void writeError(HttpServletResponse response, int status, String detail) throws IOException {
        response.setStatus(status);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.getWriter().write(MAPPER.writeValueAsString(Map.of("detail", detail)));
    }
}
