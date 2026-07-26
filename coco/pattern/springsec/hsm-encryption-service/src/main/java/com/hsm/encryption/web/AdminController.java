package com.hsm.encryption.web;

import com.hsm.encryption.audit.AuditLogger;
import com.hsm.encryption.auth.AppRegistryException;
import com.hsm.encryption.auth.AppRegistryService;
import com.hsm.encryption.crypto.KekClient;
import com.hsm.encryption.crypto.MockKekClient;
import com.hsm.encryption.dto.AppStatusRequest;
import com.hsm.encryption.dto.AppStatusResponse;
import com.hsm.encryption.dto.GrantListResponse;
import com.hsm.encryption.dto.GrantRequest;
import com.hsm.encryption.dto.GrantResponse;
import com.hsm.encryption.dto.HealthResponse;
import com.hsm.encryption.dto.RotateKekResponse;
import com.hsm.encryption.service.RotationService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Ported from app/routers/admin.py. Scope enforcement (rotate/grant/manage_apps
 * authorities) is declarative -- see hsm.security.access-rules (application.yml)
 * and com.hsm.encryption.security.SecurityConfig; a request without the needed
 * authority never reaches these methods. GET /admin/health is intentionally not
 * in the rule set -- it stays public for Kubernetes liveness/readiness probes.
 */
@RestController
public class AdminController {

    private final AppRegistryService appRegistry;
    private final KekClient kekClient;
    private final RotationService rotationService;
    private final AuditLogger auditLogger;
    private final JdbcTemplate jdbcTemplate;

    public AdminController(AppRegistryService appRegistry, KekClient kekClient, RotationService rotationService,
                            AuditLogger auditLogger, JdbcTemplate jdbcTemplate) {
        this.appRegistry = appRegistry;
        this.kekClient = kekClient;
        this.rotationService = rotationService;
        this.auditLogger = auditLogger;
        this.jdbcTemplate = jdbcTemplate;
    }

    @PostMapping("${hsm.service.api-v1-prefix}/admin/rotate-kek")
    public RotateKekResponse rotateKek(@AuthenticationPrincipal AuthenticatedCaller caller) {
        // Demo HSM stand-in must mint a new key version itself; Azure does this via
        // its own rotation policy, so the real client has no such method.
        if (kekClient instanceof MockKekClient mock) {
            mock.rotateToNewVersion();
        }

        return rotationService.rotateKek("api:" + caller.sub());
    }

    @GetMapping("${hsm.service.api-v1-prefix}/admin/health")
    public HealthResponse health() {
        boolean vaultOk = false;
        boolean dbOk = false;
        try {
            kekClient.getCurrentKekVersion();
            vaultOk = true;
        } catch (Exception e) {
            // degraded
        }
        try {
            jdbcTemplate.execute("SELECT 1");
            dbOk = true;
        } catch (Exception e) {
            // degraded
        }
        String overall = (vaultOk && dbOk) ? "ok" : "degraded";
        return new HealthResponse(overall, vaultOk, dbOk);
    }

    @PostMapping("${hsm.service.api-v1-prefix}/admin/grants")
    public ResponseEntity<GrantResponse> addGrant(@Valid @RequestBody GrantRequest body, @AuthenticationPrincipal AuthenticatedCaller caller) {
        appRegistry.addGrant(body.granteeAppId(), body.ownerAppId());
        auditLogger.log("grant_added", "app_id", caller.appId(), "sub", caller.sub(),
                "grantee_app_id", body.granteeAppId(), "owner_app_id", body.ownerAppId(), "status", "success");
        return ResponseEntity.status(HttpStatus.CREATED).body(new GrantResponse(body.granteeAppId(), body.ownerAppId()));
    }

    @DeleteMapping("${hsm.service.api-v1-prefix}/admin/grants")
    public ResponseEntity<Void> removeGrant(@Valid @RequestBody GrantRequest body, @AuthenticationPrincipal AuthenticatedCaller caller) {
        appRegistry.removeGrant(body.granteeAppId(), body.ownerAppId());
        auditLogger.log("grant_removed", "app_id", caller.appId(), "sub", caller.sub(),
                "grantee_app_id", body.granteeAppId(), "owner_app_id", body.ownerAppId(), "status", "success");
        return ResponseEntity.noContent().build();
    }

    @GetMapping("${hsm.service.api-v1-prefix}/admin/grants")
    public GrantListResponse listGrants(@AuthenticationPrincipal AuthenticatedCaller caller) {
        List<GrantResponse> grants = appRegistry.listGrants().stream()
                .map(g -> new GrantResponse(g.get("grantee_app_id"), g.get("owner_app_id")))
                .toList();
        return new GrantListResponse(grants);
    }

    /**
     * Block or restore an app. Intentionally a separate scope from 'grant' -- granting
     * a decrypt relationship and disabling another app's ability to act entirely are
     * different powers, and an incident response workflow shouldn't need both by default.
     */
    @PostMapping("${hsm.service.api-v1-prefix}/admin/apps/status")
    public AppStatusResponse setAppStatus(@Valid @RequestBody AppStatusRequest body, @AuthenticationPrincipal AuthenticatedCaller caller) {
        try {
            appRegistry.setActive(body.appId(), body.active());
        } catch (AppRegistryException e) {
            throw new ApiException(HttpStatus.NOT_FOUND, e.getMessage());
        }

        auditLogger.log("app_status_changed", "app_id", caller.appId(), "sub", caller.sub(),
                "target_app_id", body.appId(), "active", body.active(), "status", "success");
        return new AppStatusResponse(body.appId(), body.active());
    }
}
