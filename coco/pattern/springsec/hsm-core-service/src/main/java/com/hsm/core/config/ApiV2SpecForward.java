package com.hsm.core.config;

import io.swagger.v3.oas.annotations.Hidden;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * Publishes the v2 OpenAPI document under the v2 prefix, next to the API it describes
 * ({@code <api-v2-prefix>/openapi}, plus {@code .yaml}), mirroring v1's
 * {@code <api-v1-prefix>/openapi}. springdoc serves groups only below its single
 * api-docs path (the v1 one), so this forwards there. Fetching it from the v2 URL matters:
 * the document's only server is ".", which resolves against that URL.
 */
@Hidden
@Controller
@ConditionalOnProperty(prefix = "springdoc.api-docs", name = "enabled", havingValue = "true")
public class ApiV2SpecForward {

    private final String apiDocsPath;

    public ApiV2SpecForward(@Value("${springdoc.api-docs.path}") String apiDocsPath) {
        this.apiDocsPath = apiDocsPath;
    }

    @GetMapping("${hsm.service.api-v2-prefix}/openapi")
    public String json() {
        return "forward:" + apiDocsPath + "/" + OpenApiConfig.V2_GROUP;
    }

    @GetMapping("${hsm.service.api-v2-prefix}/openapi.yaml")
    public String yaml() {
        return "forward:" + apiDocsPath + ".yaml/" + OpenApiConfig.V2_GROUP;
    }
}
