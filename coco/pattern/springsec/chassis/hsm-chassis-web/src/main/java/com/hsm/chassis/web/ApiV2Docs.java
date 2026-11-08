package com.hsm.chassis.web;

import io.swagger.v3.oas.annotations.Hidden;
import org.springdoc.core.properties.AbstractSwaggerUiConfigProperties.SwaggerUrl;
import org.springdoc.core.properties.SwaggerUiConfigProperties;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Swagger UI for a service that serves two API versions ({@code hsm.chassis.web.api-prefix}
 * for v1, {@code hsm.chassis.web.api-v2-prefix} for v2): one Swagger UI page, under the v1
 * prefix as before, with a version dropdown, plus {@code <api-v2-prefix>/docs} opening it on
 * v2. Each version's document is expected at {@code <prefix>/openapi}.
 *
 * <p>Every URL stays RELATIVE, like {@link ApiDocsEntryController}, so it resolves against
 * whatever external prefix the browser used. That works as long as the two prefixes keep
 * the same relative layout outside as inside (the defaults: siblings, .../v1 and .../v2).
 */
public final class ApiV2Docs {

    static final String V1 = "v1";
    static final String V2 = "v2";

    private ApiV2Docs() {
    }

    /** Points Swagger UI at both documents (v1 selected) instead of the single {@code springdoc.swagger-ui.url}. */
    static void configure(SwaggerUiConfigProperties swaggerUi, String v1Prefix, String v2Prefix) {
        Set<SwaggerUrl> urls = new LinkedHashSet<>();
        urls.add(new SwaggerUrl(V1, "../openapi", V1));
        urls.add(new SwaggerUrl(V2, relative(v1Prefix + "/swagger-ui/", v2Prefix + "/openapi"), V2));
        swaggerUi.setUrl(null);
        swaggerUi.setUrls(urls);
        swaggerUi.setUrlsPrimaryName(V1);
    }

    /**
     * The path that leads from directory {@code fromDir} (ending in "/") to {@code to}, e.g.
     * from /api/x/v1/swagger-ui/ to /api/x/v2/openapi is ../../v2/openapi.
     */
    static String relative(String fromDir, String to) {
        List<String> from = segments(fromDir);
        List<String> target = segments(to);
        int common = 0;
        while (common < from.size() && common < target.size() - 1 && from.get(common).equals(target.get(common))) {
            common++;
        }
        StringBuilder out = new StringBuilder();
        for (int i = common; i < from.size(); i++) {
            out.append("../");
        }
        out.append(String.join("/", target.subList(common, target.size())));
        return out.toString();
    }

    private static List<String> segments(String path) {
        return new ArrayList<>(Arrays.stream(path.split("/")).filter(s -> !s.isEmpty()).toList());
    }

    /** {@code <api-v2-prefix>/docs}: Swagger UI (served under the v1 prefix) with v2 selected. Hidden from the spec. */
    @Hidden
    @RestController
    public static class EntryController {

        private final String location;

        public EntryController(String v1Prefix, String v2Prefix) {
            this.location = relative(v2Prefix + "/", v1Prefix + "/swagger-ui/index.html")
                    + "?urls.primaryName=" + URLEncoder.encode(V2, StandardCharsets.UTF_8);
        }

        @GetMapping("${" + ChassisWebAutoConfiguration.API_V2_PREFIX_PROPERTY + "}/docs")
        public ResponseEntity<Void> docs() {
            return ResponseEntity.status(HttpStatus.FOUND).header(HttpHeaders.LOCATION, location).build();
        }
    }
}
