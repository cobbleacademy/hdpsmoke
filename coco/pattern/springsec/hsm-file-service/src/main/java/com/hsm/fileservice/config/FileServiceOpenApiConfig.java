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
import io.swagger.v3.oas.models.servers.ServerVariable;
import io.swagger.v3.oas.models.servers.ServerVariables;
import org.springdoc.core.customizers.OpenApiCustomizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Document-level metadata for the generated spec, plus fix-ups annotations can't
 * express. By default the spec is served on the management port (/actuator/openapi) and
 * its committed copy, helm/hsm-file-service/openapi.yaml, ships with the chart; see
 * OpenApiContractTest for how the two are kept identical. With the "docs" profile it is
 * served on the API port instead (<api-prefix>/openapi, for Swagger UI) with a relative
 * server, so Try it out goes back through whatever external prefix the browser used.
 */
@Configuration
public class FileServiceOpenApiConfig {

    /** The version consumers see in the spec; keep in step with the chart's appVersion. */
    static final String API_VERSION = "1.0.0";

    @Bean
    public OpenAPI fileServiceOpenApi(FileServiceProperties props,
                                      @Value("${springdoc.use-management-port:false}") boolean onManagementPort) {
        Server server = onManagementPort
                // Fixed host, not the generating request's, so the committed spec is deterministic.
                // The API prefix is a server variable (paths are relative to it), so the one
                // committed contract holds for every deployment's server.api-prefix; its default
                // is whatever this deployment is configured with.
                ? new Server().url("http://hsm-file-service:8080{apiPrefix}")
                        .description("In-cluster Service, file port. Reachable only from the BFF.")
                        .variables(new ServerVariables().addServerVariable("apiPrefix", new ServerVariable()
                                ._default(props.server().apiPrefix())
                                .description("This deployment's API prefix (chart config.server.apiPrefix, env "
                                        + "FILE_SERVICE_API_PREFIX). Default " + FileServiceProperties.Server.DEFAULT_API_PREFIX + ".")))
                // Served at <prefix>/openapi: "." resolves to <external prefix>/, so ./files/{path}
                // is right behind any VirtualService prefix or rewrite.
                : new Server().url(".").description("The API prefix this document was fetched from");
        return new OpenAPI()
                .info(new Info()
                        .title("hsm-file-service")
                        .version(API_VERSION)
                        .description("""
                                Read-only service, deployed in a consumer's namespace, that serves decrypted files to that
                                consumer's BFF. Authentication is the mesh: Istio STRICT mTLS plus an AuthorizationPolicy
                                allowing only the BFF's service account, so there is no token or API key. The response body
                                is the original file's bytes; JSON appears only for errors. Full guide: FILE_SERVICE.md."""))
                .servers(List.of(server));
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
    public OpenApiCustomizer fileServiceOpenApiCustomizer(FileServiceProperties props) {
        String apiPrefix = props.server().apiPrefix();
        return openApi -> {
            // Paths relative to the API prefix (it lives in the server URL), and Spring's
            // <api-prefix>/files/** -- multi-segment, so the path may contain '/' -- published as
            // a single {path}, since OpenAPI has no multi-segment wildcard.
            Paths renamed = new Paths();
            openApi.getPaths().forEach((path, item) -> {
                if (path.endsWith("/**")) {
                    item.readOperations().forEach(op -> op.addParametersItem(pathParameter()));
                }
                String relative = path.startsWith(apiPrefix) ? path.substring(apiPrefix.length()) : path;
                renamed.addPathItem(relative.replace("/**", "/{path}"), item);
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
