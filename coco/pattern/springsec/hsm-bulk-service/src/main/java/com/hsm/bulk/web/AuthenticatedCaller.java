package com.hsm.bulk.web;

import java.util.List;

/** Duplicated from com.hsm.core.web.AuthenticatedCaller -- carries the validated identity set by JwtAppIdAuthenticationFilter. */
public record AuthenticatedCaller(String appId, String sub, List<String> scopes) {
}
