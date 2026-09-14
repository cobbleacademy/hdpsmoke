package com.hsm.core.service;

import com.hsm.core.audit.RecentEventsBuffer;
import com.hsm.core.model.KekRegistryEntry;
import com.hsm.core.repository.KekRegistryEntryRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 1 (shadow mode) -- checkReservation never rejects anything; these
 * tests confirm the branch it actually takes (audit event emitted or not).
 * See ClassificationGovernanceServiceTest for why assertions search for a
 * matching event rather than assume it's the single most-recent one
 * (RecentEventsBuffer is a shared singleton across test methods in this
 * class).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("demo")
class DekNameReservationServiceTest {

    @DynamicPropertySource
    static void overrideDatasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:mem:dekreservation-" + System.nanoTime() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
    }

    @Autowired
    private DekNameReservationService dekNameReservation;

    @Autowired
    private KekRegistryEntryRepository kekRegistryRepository;

    @Autowired
    private RecentEventsBuffer recentEvents;

    private boolean sawShadowModeConflict(String appId, String dekName, String reservedFor) {
        return recentEvents.recent(200).stream().anyMatch(event ->
                "dek_name_reservation_check".equals(event.get("event_type"))
                        && appId.equals(event.get("app_id"))
                        && dekName.equals(event.get("dek_name"))
                        && reservedFor.equals(event.get("reserved_for"))
                        && "conflict_shadow_mode".equals(event.get("status")));
    }

    @Test
    void blankDekNameIsANoOp() {
        String appId = "dnr-test-blank";
        dekNameReservation.checkReservation(appId, null);
        dekNameReservation.checkReservation(appId, "");
        dekNameReservation.checkReservation(appId, "   ");

        boolean sawAnyEventForThisApp = recentEvents.recent(200).stream()
                .anyMatch(event -> "dek_name_reservation_check".equals(event.get("event_type")) && appId.equals(event.get("app_id")));
        assertFalse(sawAnyEventForThisApp);
    }

    @Test
    void noConflictWhenNoOtherAppHasReservedIt() {
        String appId = "dnr-test-no-conflict";
        dekNameReservation.checkReservation(appId, "customers.dnr-test-unclaimed");

        boolean sawAnyEventForThisApp = recentEvents.recent(200).stream()
                .anyMatch(event -> "dek_name_reservation_check".equals(event.get("event_type")) && appId.equals(event.get("app_id")));
        assertFalse(sawAnyEventForThisApp);
    }

    @Test
    void conflictWithDifferentAppEmitsShadowModeAuditEvent() {
        String ownerAppId = "dnr-test-owner";
        String otherAppId = "dnr-test-conflicting";
        String dekName = "customers.dnr-test-reserved";
        kekRegistryRepository.save(new KekRegistryEntry(ownerAppId, dekName, KekRegistryEntry.UNSET, "hsm-master-kek"));

        dekNameReservation.checkReservation(otherAppId, dekName);

        assertTrue(sawShadowModeConflict(otherAppId, dekName, ownerAppId));
    }

    @Test
    void ownAppReservationIsNeverAConflict() {
        String appId = "dnr-test-same-app";
        String dekName = "customers.dnr-test-own";
        kekRegistryRepository.save(new KekRegistryEntry(appId, dekName, KekRegistryEntry.UNSET, "hsm-master-kek"));

        dekNameReservation.checkReservation(appId, dekName);

        boolean sawAnyEventForThisApp = recentEvents.recent(200).stream()
                .anyMatch(event -> "dek_name_reservation_check".equals(event.get("event_type")) && appId.equals(event.get("app_id")));
        assertFalse(sawAnyEventForThisApp, "an app's own kek_registry row for a dek_name it's about to mint must never be treated as a conflict");
    }

    @Test
    void classificationAndDefaultTierRowsAreNeverTreatedAsReservations() {
        // A classification-tier row (dekName unset) and a per-app-default row
        // (both unset) don't name a specific dek_name at all -- they must never
        // surface as a "reservation" conflict for ANY dek_name another app mints.
        String otherOwnerAppId = "dnr-test-tier-owner";
        String callerAppId = "dnr-test-tier-caller";
        kekRegistryRepository.save(new KekRegistryEntry(otherOwnerAppId, KekRegistryEntry.UNSET, "pii", "hsm-master-kek"));
        kekRegistryRepository.save(new KekRegistryEntry(otherOwnerAppId, KekRegistryEntry.UNSET, KekRegistryEntry.UNSET, "hsm-master-kek"));

        dekNameReservation.checkReservation(callerAppId, "customers.dnr-test-tier-check");

        boolean sawAnyEventForThisApp = recentEvents.recent(200).stream()
                .anyMatch(event -> "dek_name_reservation_check".equals(event.get("event_type")) && callerAppId.equals(event.get("app_id")));
        assertFalse(sawAnyEventForThisApp);
    }
}
