package com.hsm.encryption.security;

import com.hsm.encryption.audit.AuditLogger;
import com.hsm.encryption.auth.AppRegistryService;
import com.hsm.encryption.auth.JwtValidator;
import com.hsm.encryption.config.HsmProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
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
    public HsmAccessDeniedHandler hsmAccessDeniedHandler(
            HsmSecurityProperties securityProperties, HsmProperties properties, AuditLogger auditLogger) {
        return new HsmAccessDeniedHandler(securityProperties, properties, auditLogger);
    }

    @Bean
    public SecurityFilterChain securityFilterChain(
            HttpSecurity http,
            JwtAppIdAuthenticationFilter authFilter,
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
                .addFilterBefore(authFilter, UsernamePasswordAuthenticationFilter.class);

        return http.build();
    }
}
