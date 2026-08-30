package com.hsm.core.security;

import com.hsm.core.audit.AuditLogger;
import com.hsm.core.auth.AppRegistryService;
import com.hsm.core.auth.JwtValidator;
import com.hsm.core.config.HsmProperties;
import com.hsm.core.web.CorrelationIdFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.lang.Nullable;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

/**
 * Declarative, externally-configured authorization: hsm.security.access-rules
 * (application.yml, bound by HsmSecurityProperties) is the single source of
 * truth for which endpoints require which authority. Authentication itself is
 * handled by JwtAppIdAuthenticationFilter, inserted ahead of Spring Security's
 * own filters. Stateless bearer-token API: no CSRF, no sessions, no
 * form-login/http-basic (the demo UI sends Authorization headers via fetch(),
 * never relies on a cookie).
 */
@Configuration
public class SecurityConfig {

    @Bean
    public CorrelationIdFilter correlationIdFilter() {
        return new CorrelationIdFilter();
    }

    @Bean
    public JwtAppIdAuthenticationFilter jwtAppIdAuthenticationFilter(
            JwtValidator jwtValidator,
            AppRegistryService appRegistry,
            AuditLogger auditLogger,
            HsmSecurityProperties securityProperties,
            HsmProperties properties
    ) {
        return new JwtAppIdAuthenticationFilter(
                jwtValidator, appRegistry, auditLogger, securityProperties, properties.service().apiV1Prefix());
    }

    @Bean
    @ConditionalOnProperty(prefix = "hsm.security", name = "mtls-enabled", havingValue = "true")
    public MtlsAppIdAuthenticationFilter mtlsAppIdAuthenticationFilter(
            AppRegistryService appRegistry,
            AuditLogger auditLogger,
            HsmSecurityProperties securityProperties,
            HsmProperties properties
    ) {
        return new MtlsAppIdAuthenticationFilter(
                appRegistry, auditLogger, securityProperties, properties.service().apiV1Prefix());
    }

    @Bean
    public HsmAccessDeniedHandler hsmAccessDeniedHandler(
            HsmSecurityProperties securityProperties, HsmProperties properties, AuditLogger auditLogger) {
        return new HsmAccessDeniedHandler(securityProperties, properties, auditLogger);
    }

    @Bean
    public SecurityFilterChain securityFilterChain(
            HttpSecurity http,
            CorrelationIdFilter correlationIdFilter,
            JwtAppIdAuthenticationFilter authFilter,
            @Autowired(required = false) @Nullable MtlsAppIdAuthenticationFilter mtlsAuthFilter,
            HsmAccessDeniedHandler accessDeniedHandler,
            HsmSecurityProperties securityProperties,
            HsmProperties properties
    ) throws Exception {
        String prefix = properties.service().apiV1Prefix();

        http
                .csrf(AbstractHttpConfigurer::disable)
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .formLogin(AbstractHttpConfigurer::disable)
                .httpBasic(AbstractHttpConfigurer::disable)
                .authorizeHttpRequests(auth -> {
                    for (HsmSecurityProperties.AccessRule rule : securityProperties.accessRules()) {
                        String pattern = prefix + rule.pattern();
                        String[] authorities = rule.authorities().toArray(new String[0]);
                        for (String method : rule.methods()) {
                            auth.requestMatchers(HttpMethod.valueOf(method), pattern).hasAnyAuthority(authorities);
                        }
                    }
                    // Everything not listed in hsm.security.access-rules (health check,
                    // demo/** -- itself only registered at all under demo-mode=true --
                    // and the static demo UI) is open, matching today's behavior.
                    auth.anyRequest().permitAll();
                })
                .exceptionHandling(ex -> ex
                        .authenticationEntryPoint(new HsmAuthenticationEntryPoint())
                        .accessDeniedHandler(accessDeniedHandler)
                )
                .addFilterBefore(authFilter, UsernamePasswordAuthenticationFilter.class)
                .addFilterBefore(correlationIdFilter, JwtAppIdAuthenticationFilter.class);

        // Runs ahead of the JWT filter: if a client certificate authenticates
        // successfully, JwtAppIdAuthenticationFilter's own early-exit guard (skip if
        // SecurityContext already has an Authentication) means the request never
        // needs a bearer token at all. Only present when hsm.security.mtls-enabled=true.
        if (mtlsAuthFilter != null) {
            http.addFilterBefore(mtlsAuthFilter, JwtAppIdAuthenticationFilter.class);
        }

        return http.build();
    }
}
