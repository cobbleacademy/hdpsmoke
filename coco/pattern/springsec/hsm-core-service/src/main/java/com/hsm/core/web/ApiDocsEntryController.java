package com.hsm.core.web;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * {@code <api-v1-prefix>/docs}: the Swagger UI entry point to link to. springdoc's own
 * {@code /swagger-ui.html} redirects to an absolute internal path, which breaks behind a
 * gateway that rewrites prefixes (/api/dsec/core/v1 -> /api/sensec/hsm/v1): the browser
 * would leave for the internal prefix. This redirect is RELATIVE, so the browser resolves
 * it against the external URL it actually used. Registered only when Swagger UI is on
 * (demo profile or SPRINGDOC_ENABLED=true).
 */
@RestController
@ConditionalOnProperty(prefix = "springdoc.swagger-ui", name = "enabled", havingValue = "true")
public class ApiDocsEntryController {

    @GetMapping("${hsm.service.api-v1-prefix}/docs")
    public ResponseEntity<Void> docs() {
        // Relative to .../<prefix>/docs, so it lands on .../<prefix>/swagger-ui/index.html
        // under whatever prefix the caller used.
        return ResponseEntity.status(HttpStatus.FOUND)
                .header(HttpHeaders.LOCATION, "swagger-ui/index.html")
                .build();
    }
}
