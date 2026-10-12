package com.hsm.chassis.web;

import io.swagger.v3.oas.annotations.Hidden;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * {@code <api-prefix>/docs}: the Swagger UI entry point for developers. springdoc's own
 * {@code swagger-ui.html} redirects to an absolute internal path, which breaks behind a
 * VirtualService that rewrites prefixes; this redirect is RELATIVE, so the browser
 * resolves it against the external URL it actually used. Registered by
 * {@link ChassisWebAutoConfiguration} only when Swagger UI is enabled and the service
 * names its prefix in {@code hsm.chassis.web.api-prefix}. Hidden from the OpenAPI spec.
 */
@Hidden
@RestController
public class ApiDocsEntryController {

    @GetMapping("${" + ChassisWebAutoConfiguration.API_PREFIX_PROPERTY + "}/docs")
    public ResponseEntity<Void> docs() {
        // Relative to .../<prefix>/docs, so it lands on .../<prefix>/swagger-ui/index.html
        // under whatever prefix the caller used.
        return ResponseEntity.status(HttpStatus.FOUND)
                .header(HttpHeaders.LOCATION, "swagger-ui/index.html")
                .build();
    }
}
