package com.hsm.encryption.auth;

import java.util.Map;

/**
 * Policy Enforcement Point shim -- the HSM service is the PEP, PlainID is the PDP.
 * This interface's only contract with the rest of the codebase is
 * {@code check(endUserId, action, dataClassification, context)}.
 */
public interface PbacClient {

    /**
     * Return true if endUserId is permitted to perform action.
     *
     * @param endUserId          the logged-in user identity from the request
     * @param action             "encrypt" or "decrypt"
     * @param dataClassification used to build the resource string from resource_templates
     * @param context            extra attributes forwarded to PlainID (app_id, caller_ip, owner_app_id, etc.)
     */
    boolean check(String endUserId, String action, String dataClassification, Map<String, Object> context);
}
