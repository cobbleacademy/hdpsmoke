package com.hsm.bulk.security;

import com.hsm.bulk.audit.AuditLogger;
import com.hsm.bulk.auth.AppRegistryService;
import com.hsm.bulk.auth.JwtValidator;
import com.hsm.bulk.config.HsmBulkProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

/** Duplicated from com.hsm.core.security.SecurityConfig -- same declarative, externally-configured authorization model (hsm.security.access-rules), now covering dek_issue/dek_unwrap instead of encrypt/decrypt/rotate/grant/manage_apps. */
@Configuration
public class SecurityConfig {

    @Bean
    public JwtAppIdAuthenticationFilter jwtAppIdAuthenticationFilter(
            JwtValidator jwtValidator,
            AppRegistryService appRegistry,
            AuditLogger auditLogger,
            HsmSecurityProperties securityProperties,
            HsmBulkProperties properties
    ) {
        return new JwtAppIdAuthenticationFilter(
                jwtValidator, appRegistry, auditLogger, securityProperties, properties.service().apiV1Prefix());
    }

    @Bean
    public HsmAccessDeniedHandler hsmAccessDeniedHandler(
            HsmSecurityProperties securityProperties, HsmBulkProperties properties, AuditLogger auditLogger) {
        return new HsmAccessDeniedHandler(securityProperties, properties, auditLogger);
    }

    @Bean
    public SecurityFilterChain securityFilterChain(
            HttpSecurity http,
            JwtAppIdAuthenticationFilter authFilter,
            HsmAccessDeniedHandler accessDeniedHandler,
            HsmSecurityProperties securityProperties,
            HsmBulkProperties properties
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
