package com.hsm.bulk.security;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/** Duplicated from com.hsm.core.security.HsmSecurityProperties -- new authorities this module introduces: dek_issue, dek_unwrap. */
@ConfigurationProperties(prefix = "hsm.security")
public record HsmSecurityProperties(List<AccessRule> accessRules) {

    public record AccessRule(String pattern, List<String> methods, List<String> authorities) {
    }
}
