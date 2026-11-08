package com.hsm.fileservice.config;

import io.swagger.v3.oas.annotations.Hidden;
import jakarta.servlet.http.HttpServletRequest;
import org.springdoc.webmvc.api.OpenApiWebMvcResource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import java.util.Locale;
import java.util.Map;

/**
 * Docs profile only (spec on the API port): {@code <api-v2-prefix>/openapi}, the v2 document
 * next to the API it describes, for Swagger UI's v2 entry. Derived from springdoc's v1
 * document (FileServiceApiV2Spec). Its server is ".", which resolves against this URL --
 * i.e. the v2 prefix. On the management port (the default) there is no runtime v2 document;
 * the chart ships the committed helm/hsm-file-service/openapi-v2.yaml.
 */
@Hidden
@RestController
@ConditionalOnProperty(name = "springdoc.use-management-port", havingValue = "false")
@ConditionalOnProperty(name = "springdoc.api-docs.enabled", havingValue = "true")
public class ApiV2SpecController {

    private static final TypeReference<Map<String, Object>> MAP = new TypeReference<>() {
    };

    private final OpenApiWebMvcResource springdoc;
    private final JsonMapper json;
    private final String apiDocsPath;
    private final String v2Prefix;

    public ApiV2SpecController(OpenApiWebMvcResource springdoc, JsonMapper json, FileServiceProperties props,
                               @Value("${springdoc.api-docs.path}") String apiDocsPath) {
        this.springdoc = springdoc;
        this.json = json;
        this.apiDocsPath = apiDocsPath;
        this.v2Prefix = props.server().apiV2Prefix();
    }

    @GetMapping(value = "${hsm.file-service.server.api-v2-prefix}/openapi", produces = MediaType.APPLICATION_JSON_VALUE)
    public Map<String, Object> openapi(HttpServletRequest request, Locale locale) throws Exception {
        Map<String, Object> v1 = json.readValue(springdoc.openapiJson(request, apiDocsPath, locale), MAP);
        return FileServiceApiV2Spec.fromV1(v1, v2Prefix);
    }
}
