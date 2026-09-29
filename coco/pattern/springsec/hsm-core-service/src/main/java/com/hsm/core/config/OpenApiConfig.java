package com.hsm.core.config;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import io.swagger.v3.core.jackson.ModelResolver;
import io.swagger.v3.core.util.Json;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.servers.Server;
import org.springdoc.core.customizers.OpenApiCustomizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * Top-level OpenAPI document metadata for springdoc. Only loaded when the spec is
 * enabled (demo profile or SPRINGDOC_ENABLED=true; off in production -- see
 * application.yml). The operations themselves are generated from the controllers.
 *
 * <p>Two security schemes, both required together on every call except
 * GET /admin/health: a bearer token (Entra ID JWT, self-signed app JWT, or a demo
 * token) and the X-App-ID header naming the calling app. mTLS is a third,
 * transport-level alternative to the bearer token (AUTHORIZATION.md §1b) that
 * OpenAPI can only describe, not exercise from Swagger UI.
 */
@Configuration
@ConditionalOnProperty(prefix = "springdoc.api-docs", name = "enabled", havingValue = "true")
public class OpenApiConfig {

    static final String BEARER = "bearerAuth";
    static final String APP_ID = "appId";

    /**
     * The wire format is snake_case (spring.jackson.property-naming-strategy), but
     * swagger-core resolves schemas with its own Jackson 2 mapper, which knows
     * nothing about Spring's Jackson 3 config -- without this the spec would show
     * dataClassification where the API actually reads and writes data_classification.
     */
    @Bean
    public ModelResolver snakeCaseModelResolver() {
        return new ModelResolver(Json.mapper().copy().setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE));
    }

    /**
     * Makes the published spec independent of the URL it is reached through, so one
     * image works unchanged as demo, core and bulk behind a gateway that REWRITES
     * external prefixes (e.g. /api/dsec/core/v1 -> /api/sensec/hsm/v1). Paths are listed
     * relative to the API prefix ("/encrypt", not "/api/sensec/hsm/v1/encrypt") and the
     * only server is ".", which OpenAPI 3 resolves against the URL the spec itself was
     * fetched from -- i.e. whatever external prefix the caller used. Nothing here knows or
     * needs the external prefix; see ADMIN_OPERATIONS.md "Behind a path-rewriting gateway".
     */
    @Bean
    public OpenApiCustomizer relativeToWhereverServed(@Value("${hsm.service.api-v1-prefix}") String apiV1Prefix) {
        return openApi -> {
            Paths relative = new Paths();
            openApi.getPaths().forEach((path, item) -> relative.addPathItem(
                    path.startsWith(apiV1Prefix) ? path.substring(apiV1Prefix.length()) : path, item));
            openApi.setPaths(relative);
            openApi.setServers(List.of(new Server().url(".")
                    .description("The API prefix this document was fetched from")));
        };
    }

    @Bean
    public OpenAPI hsmCoreOpenApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("hsm-core-service")
                        .version("1.0.0")
                        .description("""
                                Centralised AES-256-GCM envelope encryption backed by Azure Key Vault Managed HSM.
                                All JSON fields are snake_case. Batch endpoints return HTTP 200 with a per-item
                                `status`; check each item. Authorization per endpoint is configured in
                                `hsm.security.access-rules` (scopes such as encrypt, decrypt, dek_issue, dek_unwrap,
                                grant). Narrative docs: java/docs/ (AUTHORIZATION.md, BULK_OPERATIONS.md,
                                ADMIN_OPERATIONS.md). mTLS client-certificate auth (AUTHORIZATION.md §1b) replaces
                                the bearer token when enabled and cannot be exercised from Swagger UI."""))
                .components(new Components()
                        .addSecuritySchemes(BEARER, new SecurityScheme()
                                .type(SecurityScheme.Type.HTTP).scheme("bearer").bearerFormat("JWT")
                                .description("Entra ID access token, self-signed app JWT, or (demo only) a demo token"))
                        .addSecuritySchemes(APP_ID, new SecurityScheme()
                                .type(SecurityScheme.Type.APIKEY).in(SecurityScheme.In.HEADER).name("X-App-ID")
                                .description("The calling app_id; must match the token's app_id / appid claim")))
                .addSecurityItem(new SecurityRequirement().addList(BEARER).addList(APP_ID));
    }
}
