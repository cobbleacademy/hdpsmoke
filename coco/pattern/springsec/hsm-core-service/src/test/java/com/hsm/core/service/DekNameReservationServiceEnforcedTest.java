package com.hsm.core.service;

import com.hsm.core.model.KekRegistryEntry;
import com.hsm.core.repository.KekRegistryEntryRepository;
import com.hsm.core.web.ApiException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Phase 2 (enforce=true) -- a SEPARATE Spring context from
 * DekNameReservationServiceTest, since DekNameReservationService reads the
 * enforce flag once at construction (see ClassificationGovernanceServiceEnforcedTest
 * for the identical reasoning).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("demo")
class DekNameReservationServiceEnforcedTest {

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:mem:dekreservation-enforced-" + System.nanoTime() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        registry.add("hsm.dek-name-reservation.enforce", () -> "true");
    }

    @Autowired
    private DekNameReservationService dekNameReservation;

    @Autowired
    private KekRegistryEntryRepository kekRegistryRepository;

    @Test
    void conflictingReservationIsRejected() {
        String ownerAppId = "dnr-enforced-owner";
        String otherAppId = "dnr-enforced-conflicting";
        String dekName = "customers.dnr-enforced-reserved";
        kekRegistryRepository.save(new KekRegistryEntry(ownerAppId, dekName, KekRegistryEntry.UNSET, "hsm-master-kek"));

        ApiException e = assertThrows(ApiException.class, () -> dekNameReservation.checkReservation(otherAppId, dekName));
        assertEquals(HttpStatus.FORBIDDEN, e.getStatus());
    }

    @Test
    void noConflictIsAllowed() {
        assertDoesNotThrow(() -> dekNameReservation.checkReservation("dnr-enforced-no-conflict", "customers.dnr-enforced-unclaimed"));
    }

    @Test
    void ownAppReservationIsAlwaysAllowedEvenWhenEnforced() {
        String appId = "dnr-enforced-same-app";
        String dekName = "customers.dnr-enforced-own";
        kekRegistryRepository.save(new KekRegistryEntry(appId, dekName, KekRegistryEntry.UNSET, "hsm-master-kek"));

        assertDoesNotThrow(() -> dekNameReservation.checkReservation(appId, dekName));
    }

    @Test
    void blankDekNameIsNeverRejectedEvenWhenEnforced() {
        assertDoesNotThrow(() -> dekNameReservation.checkReservation("dnr-enforced-blank", null));
    }
}
