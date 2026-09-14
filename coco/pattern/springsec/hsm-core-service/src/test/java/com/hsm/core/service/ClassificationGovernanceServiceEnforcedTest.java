package com.hsm.core.service;

import com.hsm.core.model.AppClassificationGrant;
import com.hsm.core.repository.AppClassificationGrantRepository;
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
 * ClassificationGovernanceServiceTest, since ClassificationGovernanceService
 * reads the enforce flag once at construction; toggling it per-test-method
 * within one context isn't possible. Confirms rejection actually happens
 * once enforcement is turned on, complementing the default (enforce=false,
 * Phase 1) shadow-mode coverage in the other test class.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("demo")
class ClassificationGovernanceServiceEnforcedTest {

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:mem:classificationgov-enforced-" + System.nanoTime() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        registry.add("hsm.classification-governance.enforce", () -> "true");
    }

    @Autowired
    private ClassificationGovernanceService classificationGovernance;

    @Autowired
    private AppClassificationGrantRepository grantRepository;

    @Test
    void unapprovedFirstMintIsRejected() {
        ApiException e = assertThrows(ApiException.class, () ->
                classificationGovernance.checkFirstMintClassification("cg-enforced-mint-unapproved", "customers.ssn", "pii"));
        assertEquals(HttpStatus.FORBIDDEN, e.getStatus());
    }

    @Test
    void approvedFirstMintIsAllowed() {
        String appId = "cg-enforced-mint-approved";
        grantRepository.save(new AppClassificationGrant(appId, "pii", "security-team"));

        assertDoesNotThrow(() -> classificationGovernance.checkFirstMintClassification(appId, "customers.ssn", "pii"));
    }

    @Test
    void blankClassificationIsNeverRejectedEvenWhenEnforced() {
        assertDoesNotThrow(() -> classificationGovernance.checkFirstMintClassification("cg-enforced-blank", "customers.ssn", null));
    }

    @Test
    void unapprovedReuseIsRejected() {
        ApiException e = assertThrows(ApiException.class, () ->
                classificationGovernance.checkReuseClassification("cg-enforced-reuse-unapproved", "customers.ssn", "pii"));
        assertEquals(HttpStatus.FORBIDDEN, e.getStatus());
    }

    @Test
    void approvedReuseIsAllowed() {
        String appId = "cg-enforced-reuse-approved";
        grantRepository.save(new AppClassificationGrant(appId, "pii", "security-team"));

        assertDoesNotThrow(() -> classificationGovernance.checkReuseClassification(appId, "customers.ssn", "pii"));
    }
}
