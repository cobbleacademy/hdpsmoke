package com.hsm.core.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.hsm.core.security.HsmSecurityProperties;
import io.swagger.v3.core.jackson.ModelResolver;
import io.swagger.v3.core.util.Json;
import io.swagger.v3.core.util.Json31;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.ObjectSchema;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.media.StringSchema;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.servers.Server;
import org.springdoc.core.customizers.GlobalOpenApiCustomizer;
import org.springdoc.core.customizers.OpenApiCustomizer;
import org.springdoc.core.models.GroupedOpenApi;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.AntPathMatcher;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    public OpenApiCustomizer relativeToWhereverServed(HsmProperties properties) {
        return openApi -> relativeTo(openApi, properties.service().apiPrefixes());
    }

    /**
     * Either prefix is stripped: springdoc lists an operation under its first mapped path
     * (the v1 one) even in the v2 document, which only selects operations that have a v2
     * mapping. Every endpoint maps both prefixes to the same handler, so the relative path
     * is the same either way.
     */
    private static void relativeTo(OpenAPI openApi, List<String> prefixes) {
        Paths relative = new Paths();
        openApi.getPaths().forEach((path, item) -> relative.addPathItem(prefixes.stream()
                .filter(path::startsWith).findFirst().map(p -> path.substring(p.length())).orElse(path), item));
        openApi.setPaths(relative);
        openApi.setServers(List.of(new Server().url(".")
                .description("The API prefix this document was fetched from")));
    }

    /**
     * The v2 document: generated from the controllers' v2 mappings (every endpoint maps
     * both prefixes), published at {@code <api-v2-prefix>/openapi} by {@link ApiV2SpecForward}.
     * springdoc's own URL for the group sits under the v1 prefix, where the relative "."
     * server would resolve to the wrong base -- use the v2 URL.
     *
     * <p>Schemas come out of the snake_case ModelResolver above (it is global, not
     * per-document), so this renames their fields back to the Java field names -- which
     * is exactly what v2 puts on the wire (ApiVersionJsonConfig). ApiV2SpecTest checks
     * every renamed field against the DTO's real record components.
     */
    @Bean
    public GroupedOpenApi apiV2Document(HsmProperties properties) {
        String v2Prefix = properties.service().apiV2Prefix();
        return GroupedOpenApi.builder()
                .group(V2_GROUP)
                .pathsToMatch(v2Prefix + "/**")
                .pathsToExclude(v2Prefix + "/demo/**")
                .addOpenApiCustomizer(openApi -> {
                    relativeTo(openApi, properties.service().apiPrefixes());
                    openApi.setInfo(new Info()
                            .title(openApi.getInfo().getTitle())
                            .version("2.0.0")
                            .description(openApi.getInfo().getDescription().replace(
                                    "All JSON fields are snake_case.",
                                    "All JSON fields are camelCase; unknown fields are rejected with 400.")));
                    // Own copy first: springdoc may share schema objects with the v1 document.
                    Components components = deepCopy(openApi.getComponents());
                    components.getSchemas().values().forEach(OpenApiConfig::camelCaseFields);
                    openApi.setComponents(components);
                })
                .build();
    }

    static final String V2_GROUP = "v2";

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void camelCaseFields(Schema schema) {
        if (schema.getProperties() != null) {
            Map<String, Schema> renamed = new LinkedHashMap<>();
            ((Map<String, Schema>) schema.getProperties()).forEach((name, property) -> {
                camelCaseFields(property);
                renamed.put(camelCase(name), property);
            });
            schema.setProperties(renamed);
        }
        if (schema.getRequired() != null) {
            schema.setRequired(((List<String>) schema.getRequired()).stream().map(OpenApiConfig::camelCase).toList());
        }
        if (schema.getItems() != null) {
            camelCaseFields(schema.getItems());
        }
    }

    static String camelCase(String snake) {
        StringBuilder out = new StringBuilder(snake.length());
        boolean upper = false;
        for (char c : snake.toCharArray()) {
            if (c == '_') {
                upper = true;
            } else {
                out.append(upper ? Character.toUpperCase(c) : c);
                upper = false;
            }
        }
        return out.toString();
    }

    private static Components deepCopy(Components components) {
        try {
            return Json31.mapper().readValue(Json31.mapper().writeValueAsString(components), Components.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("could not copy OpenAPI components", e);
        }
    }

    /**
     * Error responses on every operation of both documents, from what the code actually
     * returns: every error body is {@code {"detail": "..."}} (GlobalExceptionHandler and the
     * security filters). 401/403 only where an access rule protects the endpoint; 404 only
     * where a handler can raise one; 400/422 only where there is a request body.
     */
    @Bean
    public GlobalOpenApiCustomizer documentedErrorResponses(HsmSecurityProperties securityProperties, HsmProperties properties) {
        AntPathMatcher matcher = new AntPathMatcher();
        List<String> prefixes = properties.service().apiPrefixes();
        return openApi -> {
            openApi.getComponents().addSchemas(ERROR_SCHEMA, new ObjectSchema()
                    .addProperty("detail", new StringSchema().description("Human-readable reason"))
                    .addRequiredItem("detail"));
            openApi.getPaths().forEach((rawPath, item) -> {
                String path = prefixes.stream().filter(rawPath::startsWith).findFirst()
                        .map(p -> rawPath.substring(p.length())).orElse(rawPath);
                item.readOperationsMap().forEach((method, operation) -> {
                    boolean protectedEndpoint = securityProperties.accessRules().stream().anyMatch(rule ->
                            rule.methods().contains(method.name()) && matcher.match(rule.pattern(), path));
                    if (operation.getRequestBody() != null) {
                        addError(operation, "400", "Malformed JSON body, a wrong value type, or (v2) an unknown field");
                        addError(operation, "422", "Request validation failed (missing or invalid field, request-level rule)");
                    }
                    if (protectedEndpoint) {
                        addError(operation, "401", "Missing or invalid bearer token");
                        addError(operation, "403", "app_id claim does not match X-App-ID, app unknown or inactive, scope not permitted, or no grant");
                    }
                    if (NOT_FOUND_PATHS.contains(path)) {
                        addError(operation, "404", "The referenced record does not exist");
                    }
                    addError(operation, "500", "Unexpected server error");
                });
            });
        };
    }

    static final String ERROR_SCHEMA = "ErrorResponse";

    /** Endpoints whose handlers raise a 404 (ApiException NOT_FOUND) for the whole request. */
    private static final Set<String> NOT_FOUND_PATHS = Set.of(
            "/decrypt", "/admin/rotate-kek", "/admin/edek/{edekId}",
            "/admin/apps/status", "/admin/apps/keys", "/admin/apps/mtls-cert");

    private static void addError(Operation operation, String status, String description) {
        if (operation.getResponses() == null) {
            operation.setResponses(new ApiResponses());
        }
        operation.getResponses().putIfAbsent(status, new ApiResponse().description(description)
                .content(new Content().addMediaType("application/json",
                        new MediaType().schema(new Schema<>().$ref("#/components/schemas/" + ERROR_SCHEMA)))));
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
