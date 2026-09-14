package com.hsm.core.service;

import com.hsm.core.audit.RecentEventsBuffer;
import com.hsm.core.model.AppClassificationGrant;
import com.hsm.core.repository.AppClassificationGrantRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 1 (shadow mode) -- checkFirstMintClassification never rejects
 * anything; these tests confirm the branch it actually takes (audit event
 * emitted or not) rather than any behavior change, since there isn't one yet.
 *
 * <p>RecentEventsBuffer is a shared singleton across every test method in
 * this class (the Spring context is reused, not recreated per method), so
 * assertions search for a matching event rather than assume it's the single
 * most-recent one -- other tests' events may legitimately land after it. Each
 * test also uses its own app_id to keep DB-level grant state independent.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("demo")
class ClassificationGovernanceServiceTest {

    @DynamicPropertySource
    static void overrideDatasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:mem:classificationgov-" + System.nanoTime() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
    }

    @Autowired
    private ClassificationGovernanceService classificationGovernance;

    @Autowired
    private AppClassificationGrantRepository grantRepository;

    @Autowired
    private RecentEventsBuffer recentEvents;

    private boolean sawShadowModeEvent(String appId, String dekName, String dataClassification) {
        return recentEvents.recent(200).stream().anyMatch(event ->
                "classification_check".equals(event.get("event_type"))
                        && appId.equals(event.get("app_id"))
                        && dekName.equals(event.get("dek_name"))
                        && dataClassification.equals(event.get("data_classification"))
                        && "unapproved_shadow_mode".equals(event.get("status")));
    }

    @Test
    void blankClassificationIsANoOpRegardlessOfApproval() {
        String appId = "cg-test-blank";
        classificationGovernance.checkFirstMintClassification(appId, "customers.ssn", null);
        classificationGovernance.checkFirstMintClassification(appId, "customers.ssn", "");
        classificationGovernance.checkFirstMintClassification(appId, "customers.ssn", "   ");

        boolean sawAnyEventForThisApp = recentEvents.recent(200).stream()
                .anyMatch(event -> "classification_check".equals(event.get("event_type")) && appId.equals(event.get("app_id")));
        assertFalse(sawAnyEventForThisApp, "a blank/unset classification must never emit a classification_check event");
    }

    @Test
    void unapprovedClassificationEmitsShadowModeAuditEvent() {
        String appId = "cg-test-unapproved";
        classificationGovernance.checkFirstMintClassification(appId, "customers.ssn", "pii");

        assertTrue(sawShadowModeEvent(appId, "customers.ssn", "pii"));
    }

    @Test
    void approvedClassificationEmitsNoAuditEvent() {
        String appId = "cg-test-approved";
        grantRepository.save(new AppClassificationGrant(appId, "pii", "security-team"));

        classificationGovernance.checkFirstMintClassification(appId, "customers.ssn", "pii");

        assertFalse(sawShadowModeEvent(appId, "customers.ssn", "pii"));
    }

    @Test
    void approvalIsPerAppNotGlobal() {
        String approvedAppId = "cg-test-owner";
        String otherAppId = "cg-test-other";
        grantRepository.save(new AppClassificationGrant(approvedAppId, "pii", "security-team"));

        // A different app using the SAME classification is still unapproved.
        classificationGovernance.checkFirstMintClassification(otherAppId, "some.other.dek", "pii");

        assertTrue(sawShadowModeEvent(otherAppId, "some.other.dek", "pii"));
    }

    @Test
    void repositoryExistsCheckMatchesExactAppAndClassification() {
        String appId = "cg-test-repo-check";
        grantRepository.save(new AppClassificationGrant(appId, "pii", "security-team"));

        assertTrue(grantRepository.existsByAppIdAndDataClassification(appId, "pii"));
        assertFalse(grantRepository.existsByAppIdAndDataClassification(appId, "pci"));
        assertFalse(grantRepository.existsByAppIdAndDataClassification("cg-test-repo-check-unrelated", "pii"));
    }

    @Test
    void reuseCheckUnapprovedEmitsShadowModeAuditEventWithReuseContext() {
        String appId = "cg-test-reuse-unapproved";
        classificationGovernance.checkReuseClassification(appId, "customers.ssn", "pii");

        boolean sawReuseEvent = recentEvents.recent(200).stream().anyMatch(event ->
                "classification_check".equals(event.get("event_type"))
                        && appId.equals(event.get("app_id"))
                        && "reuse".equals(event.get("context"))
                        && "unapproved_shadow_mode".equals(event.get("status")));
        assertTrue(sawReuseEvent);
    }

    @Test
    void reuseCheckApprovedEmitsNoAuditEvent() {
        String appId = "cg-test-reuse-approved";
        grantRepository.save(new AppClassificationGrant(appId, "pii", "security-team"));

        classificationGovernance.checkReuseClassification(appId, "customers.ssn", "pii");

        assertFalse(sawShadowModeEvent(appId, "customers.ssn", "pii"));
    }

    @Test
    void addGrantIsIdempotent() {
        String appId = "cg-test-add-grant";
        AppClassificationGrant first = classificationGovernance.addGrant(appId, "pii", "security-team");
        AppClassificationGrant second = classificationGovernance.addGrant(appId, "pii", "security-team");

        assertTrue(grantRepository.existsByAppIdAndDataClassification(appId, "pii"));
        // Idempotent: the second call returns the SAME row (same grantedBy/createdAt), not a fresh one.
        org.junit.jupiter.api.Assertions.assertEquals(first.getCreatedAt(), second.getCreatedAt());
    }

    @Test
    void removeGrantDeletesRow() {
        String appId = "cg-test-remove-grant";
        classificationGovernance.addGrant(appId, "pii", "security-team");
        assertTrue(grantRepository.existsByAppIdAndDataClassification(appId, "pii"));

        classificationGovernance.removeGrant(appId, "pii");

        assertFalse(grantRepository.existsByAppIdAndDataClassification(appId, "pii"));
    }

    @Test
    void listGrantsIncludesAddedGrant() {
        String appId = "cg-test-list-grant";
        classificationGovernance.addGrant(appId, "pii", "security-team");

        assertTrue(classificationGovernance.listGrants().stream()
                .anyMatch(g -> appId.equals(g.getAppId()) && "pii".equals(g.getDataClassification())));
    }
}
