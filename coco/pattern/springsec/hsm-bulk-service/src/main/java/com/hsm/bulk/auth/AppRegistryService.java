package com.hsm.bulk.auth;

import com.hsm.bulk.model.AppDecryptGrant;
import com.hsm.bulk.model.AppRegistration;
import com.hsm.bulk.repository.AppDecryptGrantRepository;
import com.hsm.bulk.repository.AppRegistrationRepository;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Duplicated from com.hsm.core.auth.AppRegistryService (getScopes/isGranted, same
 * cache-then-DB pattern, same tables), extended with getPublicKey -- the lookup
 * DekIssueService/DekUnwrapService need to resolve which RSA-OAEP-256 key to
 * transport-wrap a DEK with for a given app_id. Follows the identical
 * cache-then-repository-fetch pattern as getScopes.
 */
@Service
public class AppRegistryService {

    private final AppRegistrationRepository registrationRepository;
    private final AppDecryptGrantRepository grantRepository;

    private final Map<String, List<String>> scopeCache = new ConcurrentHashMap<>();
    private final Map<String, String> publicKeyCache = new ConcurrentHashMap<>();
    private final Map<String, Boolean> grantCache = new ConcurrentHashMap<>();

    public AppRegistryService(AppRegistrationRepository registrationRepository, AppDecryptGrantRepository grantRepository) {
        this.registrationRepository = registrationRepository;
        this.grantRepository = grantRepository;
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

    /** The app's RSA-OAEP-256 public key (PEM), or null if the app hasn't been provisioned for dek_issue/dek_unwrap. */
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

    public void invalidate(String appId) {
        scopeCache.remove(appId);
        publicKeyCache.remove(appId);
    }

    public boolean isGranted(String granteeAppId, String ownerAppId) {
        if (granteeAppId.equals(ownerAppId)) {
            return true;
        }
        String cacheKey = granteeAppId + " " + ownerAppId;
        Boolean cached = grantCache.get(cacheKey);
        if (cached != null) {
            return cached;
        }
        boolean granted = grantRepository.existsById(new AppDecryptGrant.Key(granteeAppId, ownerAppId));
        grantCache.put(cacheKey, granted);
        return granted;
    }
}
