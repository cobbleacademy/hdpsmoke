package com.hsm.core.service;

import com.hsm.core.crypto.DekManager;
import com.hsm.core.crypto.KekClient;
import com.hsm.core.lock.JobAlreadyRunningException;
import com.hsm.core.lock.JobLock;
import com.hsm.core.model.EdekRecord;
import com.hsm.core.model.RotationStatus;
import com.hsm.core.repository.EdekRecordRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RotationService's single-runner guard: rotate/rekey/revert share one lock,
 * named-DEK rotation has its own, a held lock means 409 on the admin API, and a
 * named-DEK row that is no longer current is skipped rather than rotated twice.
 * Demo profile on H2, so the lock here is InProcessJobLock -- the cross-pod
 * Postgres behaviour is PostgresAdvisoryJobLockContainerTest's job.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@ActiveProfiles("demo")
class RotationServiceLockTest {

    private static final String TEST_KEK_NAME = "rotation-lock-test-kek";

    @DynamicPropertySource
    static void overrideDatasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:mem:rotationlock-" + System.nanoTime() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
    }

    @Autowired
    private RotationService rotationService;

    @Autowired
    private JobLock jobLock;

    @Autowired
    private EdekRecordRepository edekRecordRepository;

    @Autowired
    private KekClient kekClient;

    @Autowired
    private TestRestTemplate rest;

    @Value("${hsm.service.api-v1-prefix}")
    private String apiPrefix;

    /** Holds {@code key} on a background thread -- standing in for another pod mid-job -- while {@code body} runs. */
    private void whileHeldElsewhere(String key, Runnable body) throws InterruptedException {
        CountDownLatch acquired = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread holder = new Thread(() -> jobLock.tryRun(key, () -> {
            acquired.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return true;
        }));
        holder.start();
        assertTrue(acquired.await(5, TimeUnit.SECONDS));
        try {
            body.run();
        } finally {
            release.countDown();
            holder.join(5000);
        }
    }

    private UUID seedNamedDek(String dekName) {
        KekClient.WrapResult wrapResult = kekClient.wrapDek(DekManager.generateDek(), TEST_KEK_NAME);
        UUID edekId = UUID.randomUUID();
        edekRecordRepository.save(new EdekRecord(
                edekId, "rotation-lock-app", Base64.getEncoder().encodeToString(wrapResult.edekBytes()),
                wrapResult.kekVersion(), TEST_KEK_NAME, DekManager.ALGORITHM, "utf8", null, null, dekName));
        return edekId;
    }

    @Test
    void kekRotateRekeyAndRevertAreAllRejectedWhileTheKekWrapLockIsHeld() throws InterruptedException {
        whileHeldElsewhere(RotationService.KEK_WRAP_LOCK, () -> {
            JobAlreadyRunningException rotate = assertThrows(JobAlreadyRunningException.class,
                    () -> rotationService.rotateKek("test"));
            assertEquals(HttpStatus.CONFLICT, rotate.getStatus());
            assertThrows(JobAlreadyRunningException.class,
                    () -> rotationService.rekey("kek-a", "kek-b", "test"));
            assertThrows(JobAlreadyRunningException.class,
                    () -> rotationService.revertRekey("kek-b", "test"));
        });

        // Lock released -- the same call now runs.
        rotationService.rotateKek("test");
    }

    @Test
    void namedDekRotationHasItsOwnLock() throws InterruptedException {
        whileHeldElsewhere(RotationService.KEK_WRAP_LOCK, () -> rotationService.rotateNamedDeks(720));

        whileHeldElsewhere(RotationService.NAMED_DEK_ROTATION_LOCK, () ->
                assertThrows(JobAlreadyRunningException.class, () -> rotationService.rotateNamedDeks(720)));
    }

    @Test
    void adminRotateKekReturns409WhileARotationIsRunning() throws InterruptedException {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", "Bearer demo-token-ops-admin");
        headers.set("X-App-ID", "ops-admin");

        whileHeldElsewhere(RotationService.KEK_WRAP_LOCK, () -> {
            @SuppressWarnings("rawtypes")
            ResponseEntity<Map> response = rest.exchange(
                    apiPrefix + "/admin/rotate-kek", HttpMethod.POST, new HttpEntity<>(headers), Map.class);
            assertEquals(HttpStatus.CONFLICT, response.getStatusCode());
            assertTrue(String.valueOf(response.getBody().get("detail")).contains("already running"));
        });
    }

    @Test
    void namedDekRowAlreadyRotatedElsewhereIsSkippedNotRotatedAgain() {
        String dekName = "rotation-lock-dek-" + System.nanoTime();
        UUID original = seedNamedDek(dekName);

        assertTrue(rotationService.rotateNamedDekIfStillCurrent(original));
        long rowsAfterFirstRotation = edekRecordRepository.count();
        EdekRecord rotated = edekRecordRepository.findById(original).orElseThrow();
        assertEquals(RotationStatus.ROTATED, rotated.getRotationStatus());

        // A second runner holding a stale copy of the same candidate must back off cleanly.
        assertFalse(rotationService.rotateNamedDekIfStillCurrent(original));
        assertEquals(rowsAfterFirstRotation, edekRecordRepository.count());
        assertTrue(edekRecordRepository.findByCurrentDekName(dekName).isPresent());
    }
}
