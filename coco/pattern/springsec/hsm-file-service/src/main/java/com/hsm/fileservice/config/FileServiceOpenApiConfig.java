package com.hsm.fileservice.config;

import com.hsm.fileservice.web.ErrorCode;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.media.StringSchema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.parameters.PathParameter;
import io.swagger.v3.oas.models.servers.Server;
import org.springdoc.core.customizers.OpenApiCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Document-level metadata for the generated spec, plus two fix-ups annotations can't
 * express. The spec is served on the management port (/actuator/openapi) and its
 * committed copy, helm/hsm-file-service/openapi.yaml, ships with the chart; see
 * OpenApiContractTest for how the two are kept identical.
 */
@Configuration
public class FileServiceOpenApiConfig {

    /** The version consumers see in the spec; keep in step with the chart's appVersion. */
    static final String API_VERSION = "1.0.0";

    @Bean
    public OpenAPI fileServiceOpenApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("hsm-file-service")
                        .version(API_VERSION)
                        .description("""
                                Read-only service, deployed in a consumer's namespace, that serves decrypted files to that
                                consumer's BFF. Authentication is the mesh: Istio STRICT mTLS plus an AuthorizationPolicy
                                allowing only the BFF's service account, so there is no token or API key. The response body
                                is the original file's bytes; JSON appears only for errors. Full guide: FILE_SERVICE.md."""))
                // Fixed, not the generating request's host, so the committed spec is deterministic.
                .servers(List.of(new Server().url("http://hsm-file-service:8080")
                        .description("In-cluster Service, file port. Reachable only from the BFF.")));
    }

    private static Parameter pathParameter() {
        return new PathParameter()
                .name("path")
                .required(true)
                .description("File path below store.root; may contain '/'. Must fall under access.allowed-path-prefixes. "
                        + "Max 1024 chars; no empty, '.' or '..' segments, backslashes or control characters; "
                        + "percent-decoded as a URI path ('+' stays '+').")
                .example("tenant-a/2026/report.pdf")
                .schema(new StringSchema().maxLength(1024));
    }

    @Bean
    public OpenApiCustomizer fileServiceOpenApiCustomizer() {
        return openApi -> {
            // Spring maps the endpoint as /v1/files/** so the path may contain '/';
            // OpenAPI has no multi-segment wildcard, so publish it as a single {path}.
            Paths renamed = new Paths();
            openApi.getPaths().forEach((path, item) -> {
                if (path.endsWith("/**")) {
                    item.readOperations().forEach(op -> op.addParametersItem(pathParameter()));
                }
                renamed.addPathItem(path.replace("/**", "/{path}"), item);
            });
            openApi.setPaths(renamed);

            // error_code allowed values come from the enum, so a new code can't be forgotten in the spec.
            Map<String, Schema> schemas = openApi.getComponents().getSchemas();
            Schema<?> error = schemas.get("ErrorResponse");
            if (error != null && error.getProperties() != null) {
                @SuppressWarnings("unchecked")
                Schema<String> code = (Schema<String>) error.getProperties().get("error_code");
                code.setEnum(Arrays.stream(ErrorCode.values()).map(ErrorCode::code).toList());
            }
        };
    }
}
