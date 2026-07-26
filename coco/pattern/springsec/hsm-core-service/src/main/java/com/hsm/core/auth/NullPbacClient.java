package com.hsm.core.auth;

import java.util.Map;

/** No-op -- always permits. Used in demo mode and when pbac.enabled=false. */
public class NullPbacClient implements PbacClient {

    @Override
    public boolean check(String endUserId, String action, String dataClassification, Map<String, Object> context) {
        return true;
    }
}
