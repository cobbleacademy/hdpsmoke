package com.hsm.core.service;

import com.hsm.core.crypto.DekManager;
import com.hsm.core.crypto.KekClient;
import com.hsm.core.model.EdekRecord;
import com.hsm.core.model.RotationStatus;
import com.hsm.core.repository.EdekRecordRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Named-DEK rotation keeps going past a failing row, and stops early only when
 * failures look systemic ({@link RotationService#MAX_CONSECUTIVE_ROW_FAILURES} in a
 * row). The KEK client is a spy that fails every wrap under {@link #FAILING_KEK}.
 * Fresh context and H2 database per test, so each test sees only its own rows.
 */
@SpringBootTest
@ActiveProfiles("demo")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class NamedDekRotationRowFailureTest {

    private static final String OK_KEK = "row-failure-ok-kek";
    private static final String FAILING_KEK = "row-failure-failing-kek";

    @DynamicPropertySource
    static void overrideDatasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:mem:rowfailure-" + System.nanoTime() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
    }

    @Autowired
    private RotationService rotationService;

    @Autowired
    private EdekRecordRepository edekRecordRepository;

    @MockitoSpyBean
    private KekClient kekClient;

    @BeforeEach
    void failWrapsUnderFailingKek() {
        doThrow(new IllegalStateException("simulated Managed HSM failure"))
                .when(kekClient).wrapDek(any(), eq(FAILING_KEK));
    }

    /** Wrapped under OK_KEK (seeding must succeed) but recorded under kekName, which rotation re-wraps with. */
    private UUID seedNamedDek(String kekName) {
        KekClient.WrapResult wrapResult = kekClient.wrapDek(DekManager.generateDek(), OK_KEK);
        UUID edekId = UUID.randomUUID();
        edekRecordRepository.save(new EdekRecord(
                edekId, "row-failure-app", Base64.getEncoder().encodeToString(wrapResult.edekBytes()),
                wrapResult.kekVersion(), kekName, DekManager.ALGORITHM, "utf8", null, null,
                "row-failure-dek-" + UUID.randomUUID()));
        return edekId;
    }

    private RotationStatus statusOf(UUID edekId) {
        return edekRecordRepository.findById(edekId).orElseThrow().getRotationStatus();
    }

    @Test
    void failingRowIsSkippedAndTheRestOfTheSweepStillRotates() {
        UUID first = seedNamedDek(OK_KEK);
        UUID failing = seedNamedDek(FAILING_KEK);
        UUID last = seedNamedDek(OK_KEK);

        int rotated = rotationService.rotateNamedDeks(0);

        assertEquals(2, rotated);
        assertEquals(RotationStatus.ROTATED, statusOf(first));
        assertEquals(RotationStatus.ROTATED, statusOf(last));
        assertEquals(RotationStatus.CURRENT, statusOf(failing), "failed row is rolled back and left for the next run");
    }

    @Test
    void sweepStopsAfterTooManyConsecutiveFailures() {
        List<UUID> failing = new ArrayList<>();
        for (int i = 0; i < RotationService.MAX_CONSECUTIVE_ROW_FAILURES + 5; i++) {
            failing.add(seedNamedDek(FAILING_KEK));
        }
        clearInvocations(kekClient);

        int rotated = rotationService.rotateNamedDeks(0);

        assertEquals(0, rotated);
        verify(kekClient, times(RotationService.MAX_CONSECUTIVE_ROW_FAILURES)).wrapDek(any(), eq(FAILING_KEK));
        failing.forEach(id -> assertEquals(RotationStatus.CURRENT, statusOf(id)));
    }
}
