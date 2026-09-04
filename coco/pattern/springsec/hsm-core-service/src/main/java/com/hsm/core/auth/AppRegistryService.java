package com.hsm.core.auth;

import com.hsm.core.model.AppDekGrant;
import com.hsm.core.model.AppGrant;
import com.hsm.core.model.AppRegistration;
import com.hsm.core.repository.AppDekGrantRepository;
import com.hsm.core.repository.AppGrantRepository;
import com.hsm.core.repository.AppRegistrationRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * App registry -- maps app_id to permitted scopes. Backed by the same DB as the
 * EDEK store so registrations survive restarts; an in-process cache keeps the hot
 * path fast. Ported from app/auth/app_registry.py's AppRegistry.
 */
@Service
public class AppRegistryService {

    private final AppRegistrationRepository registrationRepository;
    private final AppGrantRepository grantRepository;
    private final AppDekGrantRepository dekGrantRepository;

    private final Map<String, List<String>> scopeCache = new ConcurrentHashMap<>();
    private final Map<String, String> publicKeyCache = new ConcurrentHashMap<>();
    private final Map<String, String> signingKeyCache = new ConcurrentHashMap<>();
    private final Map<String, String> mtlsFingerprintCache = new ConcurrentHashMap<>();
    private final Map<String, Boolean> grantCache = new ConcurrentHashMap<>();
    private final Map<String, Boolean> dekGrantCache = new ConcurrentHashMap<>();

    public AppRegistryService(AppRegistrationRepository registrationRepository, AppGrantRepository grantRepository,
                               AppDekGrantRepository dekGrantRepository) {
        this.registrationRepository = registrationRepository;
        this.grantRepository = grantRepository;
        this.dekGrantRepository = dekGrantRepository;
    }

    public List<String> getScopes(String appId) throws AppRegistryException {
        List<String> cached = scopeCache.get(appId);
        if (cached != null) {
            return cached;
        }
        AppRegistration row = registrationRepository.findById(appId).orElse(null);
        if (row == null || !row.isActive()) {
            throw new AppRegistryException("Unknown or inactive app_id: " + appId);
        }
        List<String> scopes = new ArrayList<>();
        for (String s : row.getAllowedScopes().split(",")) {
            scopes.add(s.strip());
        }
        scopeCache.put(appId, scopes);
        return scopes;
    }

    /** The app's RSA public key (PEM), for DekIssueService/DekUnwrapService to transport-wrap a DEK -- see TransportWrapper. Null if the app hasn't been provisioned for dek_issue/dek_unwrap. */
    public String getPublicKey(String appId) throws AppRegistryException {
        String cached = publicKeyCache.get(appId);
        if (cached != null) {
            return cached;
        }
        AppRegistration row = registrationRepository.findById(appId).orElse(null);
        if (row == null || !row.isActive()) {
            throw new AppRegistryException("Unknown or inactive app_id: " + appId);
        }
        String pem = row.getPublicKeyPem();
        if (pem == null || pem.isBlank()) {
            return null;
        }
        publicKeyCache.put(appId, pem);
        return pem;
    }

    /**
     * The RSA public key SelfSignedAppKeyJwtValidator verifies this app's self-issued
     * bearer JWTs against. Falls back to the DEK-transport key (getPublicKey) when no
     * dedicated signing key is registered -- the legacy one-keypair switch, see
     * AppRegistration.signingPublicKeyPem's javadoc. Null only when NEITHER key is
     * registered.
     */
    public String getSigningPublicKey(String appId) throws AppRegistryException {
        String cached = signingKeyCache.get(appId);
        if (cached != null) {
            return cached;
        }
        AppRegistration row = registrationRepository.findById(appId).orElse(null);
        if (row == null || !row.isActive()) {
            throw new AppRegistryException("Unknown or inactive app_id: " + appId);
        }
        String pem = row.getSigningPublicKeyPem();
        if (pem == null || pem.isBlank()) {
            return getPublicKey(appId);
        }
        signingKeyCache.put(appId, pem);
        return pem;
    }

    /**
     * The SHA-256 fingerprint (hex) of this app's registered mTLS client certificate,
     * for MtlsAppIdAuthenticationFilter to compare against whatever certificate was
     * actually presented at the TLS handshake. Null if the app hasn't registered one
     * -- unlike getSigningPublicKey, there is no fallback; mTLS is simply unavailable
     * for that app until one is registered via POST /admin/apps/mtls-cert.
     */
    public String getMtlsCertFingerprint(String appId) throws AppRegistryException {
        String cached = mtlsFingerprintCache.get(appId);
        if (cached != null) {
            return cached;
        }
        AppRegistration row = registrationRepository.findById(appId).orElse(null);
        if (row == null || !row.isActive()) {
            throw new AppRegistryException("Unknown or inactive app_id: " + appId);
        }
        String fingerprint = row.getMtlsCertFingerprint();
        if (fingerprint == null || fingerprint.isBlank()) {
            return null;
        }
        mtlsFingerprintCache.put(appId, fingerprint);
        return fingerprint;
    }

    /** Provisions this app's mTLS client certificate fingerprint -- POST /admin/apps/mtls-cert. */
    @Transactional
    public AppRegistration updateMtlsCertFingerprint(String appId, String fingerprint) throws AppRegistryException {
        AppRegistration row = registrationRepository.findById(appId).orElse(null);
        if (row == null) {
            throw new AppRegistryException("Unknown app_id: " + appId);
        }
        row.setMtlsCertFingerprint(fingerprint);
        AppRegistration saved = registrationRepository.save(row);
        invalidate(appId);
        return saved;
    }

    /**
     * Provisions this app's encryption and/or signing public key -- POST /admin/apps/keys.
     * Either argument may be null to leave that key unchanged; at least one must be
     * non-null (enforced by the caller, AdminController, before this is invoked).
     */
    @Transactional
    public AppRegistration updateKeys(String appId, String encryptionPublicKeyPem, String signingPublicKeyPem) throws AppRegistryException {
        AppRegistration row = registrationRepository.findById(appId).orElse(null);
        if (row == null) {
            throw new AppRegistryException("Unknown app_id: " + appId);
        }
        if (encryptionPublicKeyPem != null) {
            row.setPublicKeyPem(encryptionPublicKeyPem);
        }
        if (signingPublicKeyPem != null) {
            row.setSigningPublicKeyPem(signingPublicKeyPem);
        }
        AppRegistration saved = registrationRepository.save(row);
        invalidate(appId);
        return saved;
    }

    public void invalidate(String appId) {
        scopeCache.remove(appId);
        publicKeyCache.remove(appId);
        signingKeyCache.remove(appId);
        mtlsFingerprintCache.remove(appId);
    }

    /**
     * Block or restore an app. Must invalidate the scope cache in the same
     * operation -- without this, a deactivated app's already-cached scopes keep
     * working until something else evicts them, silently defeating the block.
     */
    @Transactional
    public void setActive(String appId, boolean active) throws AppRegistryException {
        AppRegistration row = registrationRepository.findById(appId).orElse(null);
        if (row == null) {
            throw new AppRegistryException("Unknown app_id: " + appId);
        }
        row.setActive(active);
        registrationRepository.save(row);
        invalidate(appId);
    }

    /**
     * True if granteeAppId may act (for the given scope -- "encrypt" or "decrypt") on a
     * resource owned by ownerAppId. Same-app is always true. Otherwise checked in order,
     * cheapest/broadest first: a coarse AppGrant (covers every resource ownerAppId owns
     * for this scope) short-circuits before ever checking the fine-grained table. If no
     * coarse grant applies, falls through to an AppDekGrant scoped to this specific
     * dekName -- skipped entirely when dekName is null/blank (e.g. an unnamed legacy
     * EDEK has nothing a fine-grained, name-scoped grant could ever match).
     */
    public boolean isGranted(String granteeAppId, String ownerAppId, String scope, String dekName) {
        if (granteeAppId.equals(ownerAppId)) {
            return true;
        }
        if (isCoarseGranted(granteeAppId, ownerAppId, scope)) {
            return true;
        }
        if (dekName == null || dekName.isBlank()) {
            return false;
        }
        return isDekGranted(granteeAppId, ownerAppId, dekName, scope);
    }

    private boolean isCoarseGranted(String granteeAppId, String ownerAppId, String scope) {
        String cacheKey = grantCacheKey(granteeAppId, ownerAppId, scope);
        Boolean cached = grantCache.get(cacheKey);
        if (cached != null) {
            return cached;
        }
        boolean granted = grantRepository.existsById(new AppGrant.Key(granteeAppId, ownerAppId, scope));
        grantCache.put(cacheKey, granted);
        return granted;
    }

    private boolean isDekGranted(String granteeAppId, String ownerAppId, String dekName, String scope) {
        String cacheKey = dekGrantCacheKey(granteeAppId, ownerAppId, dekName, scope);
        Boolean cached = dekGrantCache.get(cacheKey);
        if (cached != null) {
            return cached;
        }
        boolean granted = dekGrantRepository.existsById(new AppDekGrant.Key(granteeAppId, ownerAppId, dekName, scope));
        dekGrantCache.put(cacheKey, granted);
        return granted;
    }

    @Transactional
    public AppGrant addGrant(String granteeAppId, String ownerAppId, String scope) {
        AppGrant.Key key = new AppGrant.Key(granteeAppId, ownerAppId, scope);
        AppGrant existing = grantRepository.findById(key).orElse(null);
        AppGrant saved = existing != null ? existing : grantRepository.save(new AppGrant(granteeAppId, ownerAppId, scope));
        grantCache.put(grantCacheKey(granteeAppId, ownerAppId, scope), true);
        return saved;
    }

    @Transactional
    public void removeGrant(String granteeAppId, String ownerAppId, String scope) {
        grantRepository.deleteById(new AppGrant.Key(granteeAppId, ownerAppId, scope));
        grantCache.remove(grantCacheKey(granteeAppId, ownerAppId, scope));
    }

    public List<AppGrant> listGrants() {
        return grantRepository.findAll();
    }

    @Transactional
    public AppDekGrant addDekGrant(String granteeAppId, String ownerAppId, String dekName, String scope) {
        AppDekGrant.Key key = new AppDekGrant.Key(granteeAppId, ownerAppId, dekName, scope);
        AppDekGrant existing = dekGrantRepository.findById(key).orElse(null);
        AppDekGrant saved = existing != null ? existing : dekGrantRepository.save(new AppDekGrant(granteeAppId, ownerAppId, dekName, scope));
        dekGrantCache.put(dekGrantCacheKey(granteeAppId, ownerAppId, dekName, scope), true);
        return saved;
    }

    @Transactional
    public void removeDekGrant(String granteeAppId, String ownerAppId, String dekName, String scope) {
        dekGrantRepository.deleteById(new AppDekGrant.Key(granteeAppId, ownerAppId, dekName, scope));
        dekGrantCache.remove(dekGrantCacheKey(granteeAppId, ownerAppId, dekName, scope));
    }

    public List<AppDekGrant> listDekGrants() {
        return dekGrantRepository.findAll();
    }

    private static String grantCacheKey(String granteeAppId, String ownerAppId, String scope) {
        return granteeAppId + " " + ownerAppId + " " + scope;
    }

    private static String dekGrantCacheKey(String granteeAppId, String ownerAppId, String dekName, String scope) {
        return granteeAppId + " " + ownerAppId + " " + dekName + " " + scope;
    }
}
