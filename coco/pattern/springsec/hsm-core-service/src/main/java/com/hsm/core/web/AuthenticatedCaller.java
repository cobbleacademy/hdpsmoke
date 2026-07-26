package com.hsm.core.web;

import java.util.List;

/** Resolved once per request by AuthenticatedCallerResolver; carries validated identity. */
public record AuthenticatedCaller(String appId, String sub, List<String> scopes) {
}
