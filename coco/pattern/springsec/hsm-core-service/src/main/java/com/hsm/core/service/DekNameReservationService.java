package com.hsm.core.service;

import com.hsm.core.audit.AuditLogger;
import com.hsm.core.config.HsmProperties;
import com.hsm.core.model.KekRegistryEntry;
import com.hsm.core.repository.KekRegistryEntryRepository;
import com.hsm.core.web.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.Optional;

/**
 * Confirms a fresh dek_name mint doesn't collide with a DIFFERENT app's
 * exact-dek_name kek_registry reservation. A kek_registry row of the shape
 * (app_id, dek_name, '') -- tier 1 in KekRegistryService's 3-tier
 * resolution, dek_name explicitly set -- now carries dek_name-reservation
 * intent for that specific dek_name, not just a KEK-selection preference.
 * The other two tiers (dek_name unset: classification-level or per-app
 * default) carry no such intent, since they don't name a specific dek_name
 * at all -- only tier-1 rows are ever consulted here.
 *
 * <p>Same phased rollout as ClassificationGovernanceService, gated by
 * {@code hsm.dek-name-reservation.enforce} (default false): shadow mode
 * logs a conflict without rejecting; true rejects with 403. Flip only after
 * confirming, via the shadow-mode logs, that no dek_name currently being
 * minted by one app collides with an EXISTING kek_registry row already
 * registered to a different app -- flipping on a table whose rows were
 * created before this check existed could otherwise immediately reject a
 * first-time mint that was working fine yesterday.
 *
 * <p>Never applies to same-app minting (an app is always free to mint its
 * own dek_name, regardless of anyone else's kek_registry rows) or to
 * unnamed encrypts (dek_name blank -- nothing to reserve). Consulted only
 * on first mint, the same point KekRegistryService's own resolution
 * happens -- once a dek_name has an EdekRecord, dek_name-level access is
 * governed by AppRegistryService's grant model instead (app_grants/
 * app_dek_grants), which this does not replace or duplicate.
 */
@Service
public class DekNameReservationService {

    private static final Logger log = LoggerFactory.getLogger(DekNameReservationService.class);

    private final KekRegistryEntryRepository repository;
    private final AuditLogger auditLogger;
    private final boolean enforce;

    public DekNameReservationService(KekRegistryEntryRepository repository, AuditLogger auditLogger, HsmProperties properties) {
        this.repository = repository;
        this.auditLogger = auditLogger;
        this.enforce = properties.dekNameReservation().enforce();
    }

    /** Call before minting a fresh dek_name (no existing EdekRecord for it yet). A blank dekName is always a no-op. */
    public void checkReservation(String appId, String dekName) {
        if (dekName == null || dekName.isBlank()) {
            return;
        }

        Optional<KekRegistryEntry> reservedByOther = repository.findFirstByDekNameAndDataClassificationAndAppIdNot(
                dekName, KekRegistryEntry.UNSET, appId);
        if (reservedByOther.isEmpty()) {
            return;
        }
        String reservedFor = reservedByOther.get().getAppId();

        if (enforce) {
            auditLogger.log("dek_name_reservation_check",
                    "app_id", appId, "dek_name", dekName, "reserved_for", reservedFor, "status", "denied");
            throw new ApiException(HttpStatus.FORBIDDEN,
                    "dek_name '" + dekName + "' is reserved for app '" + reservedFor + "' in kek_registry");
        }

        log.warn("dek_name_reservation_shadow_mode_conflict app_id={} dek_name={} reserved_for={}", appId, dekName, reservedFor);
        auditLogger.log("dek_name_reservation_check",
                "app_id", appId, "dek_name", dekName, "reserved_for", reservedFor, "status", "conflict_shadow_mode");
    }
}
