package com.hsm.fileservice.web;

import com.hsm.fileservice.config.FileServiceProperties;
import io.swagger.v3.oas.annotations.Hidden;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * {@code <api-prefix>/docs}: the Swagger UI entry point for developers (same design as
 * hsm-core-service). springdoc's own {@code swagger-ui.html} redirects to an absolute
 * internal path, which breaks behind a VirtualService that rewrites prefixes; this
 * redirect is RELATIVE, so the browser resolves it against the external URL it used.
 * Registered only with the "docs" profile (chart docs.enabled), which turns Swagger UI on.
 */
@Hidden
@RestController
@ConditionalOnProperty(prefix = "springdoc.swagger-ui", name = "enabled", havingValue = "true")
public class ApiDocsEntryController {

    @GetMapping("${hsm.file-service.server.api-prefix:" + FileServiceProperties.Server.DEFAULT_API_PREFIX + "}/docs")
    public ResponseEntity<Void> docs() {
        // Relative to .../<prefix>/docs, so it lands on .../<prefix>/swagger-ui/index.html.
        return ResponseEntity.status(HttpStatus.FOUND)
                .header(HttpHeaders.LOCATION, "swagger-ui/index.html")
                .build();
    }
}
