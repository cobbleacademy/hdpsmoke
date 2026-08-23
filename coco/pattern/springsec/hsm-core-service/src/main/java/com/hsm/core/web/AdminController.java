package com.hsm.core.web;

import com.hsm.core.audit.AuditLogger;
import com.hsm.core.auth.AppRegistryException;
import com.hsm.core.auth.AppRegistryService;
import com.hsm.core.config.HsmProperties;
import com.hsm.core.crypto.KekClient;
import com.hsm.core.crypto.MockKekClient;
import com.hsm.core.dto.AppStatusRequest;
import com.hsm.core.dto.AppStatusResponse;
import com.hsm.core.dto.GrantListResponse;
import com.hsm.core.dto.GrantRequest;
import com.hsm.core.dto.GrantResponse;
import com.hsm.core.dto.HealthResponse;
import com.hsm.core.dto.RekeyRequest;
import com.hsm.core.dto.RekeyResponse;
import com.hsm.core.dto.RevertRekeyRequest;
import com.hsm.core.dto.RotateKekResponse;
import com.hsm.core.model.AppDecryptGrant;
import com.hsm.core.service.RotationService;
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
 * and com.hsm.core.security.SecurityConfig; a request without the needed
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
    private final String healthCheckKekName;

    public AdminController(AppRegistryService appRegistry, KekClient kekClient, RotationService rotationService,
                            AuditLogger auditLogger, JdbcTemplate jdbcTemplate, HsmProperties hsmProperties) {
        this.appRegistry = appRegistry;
        this.kekClient = kekClient;
        this.rotationService = rotationService;
        this.auditLogger = auditLogger;
        this.jdbcTemplate = jdbcTemplate;
        // The legacy single-KEK config value, repurposed purely as a reachability
        // ping target here -- health has never been about one specific business
        // KEK, just "is the vault/HSM endpoint up," so any resolvable key proves that.
        this.healthCheckKekName = hsmProperties.azure().kekName();
    }

    @PostMapping("${hsm.service.api-v1-prefix}/admin/rotate-kek")
    public RotateKekResponse rotateKek(@AuthenticationPrincipal AuthenticatedCaller caller) {
        // Demo HSM stand-in must mint a new key version itself; Azure does this via
        // its own rotation policy, so the real client has no such method. Multi-KEK
        // aware: every distinct demo key this instance has created so far gets a
        // fresh version, so the grouped sweep below has something to converge each
        // one's lagging EDEKs to -- not just "the one KEK" the way this worked
        // before kek_name existed.
        if (kekClient instanceof MockKekClient mock) {
            for (String kekName : mock.getKnownKekNames()) {
                mock.rotateToNewVersion(kekName);
            }
        }

        return rotationService.rotateKek("api:" + caller.sub());
    }

    /**
     * Manual, explicit -- compromise response or key decommissioning, not part
     * of any schedule. Moves every current EDEK under fromKekName to
     * toKekName; see RotationService.rekey.
     */
    @PostMapping("${hsm.service.api-v1-prefix}/admin/rekey-kek")
    public RekeyResponse rekeyKek(@Valid @RequestBody RekeyRequest body, @AuthenticationPrincipal AuthenticatedCaller caller) {
        return rotationService.rekey(body.fromKekName(), body.toKekName(), "api:" + caller.sub());
    }

    /** Undoes the most recent rekey into kekName -- see RotationService.revertRekey. */
    @PostMapping("${hsm.service.api-v1-prefix}/admin/rekey-kek/revert")
    public RekeyResponse revertRekeyKek(@Valid @RequestBody RevertRekeyRequest body, @AuthenticationPrincipal AuthenticatedCaller caller) {
        return rotationService.revertRekey(body.kekName(), "api:" + caller.sub());
    }

    @GetMapping("${hsm.service.api-v1-prefix}/admin/health")
    public HealthResponse health() {
        boolean vaultOk = false;
        boolean dbOk = false;
        try {
            // Reachability check only -- any resolvable key proves the vault/HSM
            // endpoint itself is up, this isn't about a specific business KEK.
            kekClient.getCurrentKekVersion(healthCheckKekName);
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
        AppDecryptGrant grant = appRegistry.addGrant(body.granteeAppId(), body.ownerAppId());
        auditLogger.log("grant_added", "app_id", caller.appId(), "sub", caller.sub(),
                "grantee_app_id", body.granteeAppId(), "owner_app_id", body.ownerAppId(), "status", "success");
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(new GrantResponse(grant.getGranteeAppId(), grant.getOwnerAppId(), grant.getCreatedAt()));
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
                .map(g -> new GrantResponse(g.getGranteeAppId(), g.getOwnerAppId(), g.getCreatedAt()))
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
