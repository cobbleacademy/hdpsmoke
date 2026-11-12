package com.hsm.core.service;

import com.hsm.core.crypto.KekClient;
import com.hsm.core.crypto.MockKekClient;
import com.hsm.core.dto.RotateKekResponse;
import com.hsm.core.model.EdekRecord;
import com.hsm.core.repository.EdekRecordRepository;
import com.hsm.crypto.DekManager;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.time.Duration;
import java.util.Base64;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;

/**
 * KEK rotation and rekey must label every row with the KEK version it was
 * actually wrapped under (the version Key Vault reports back from the wrap), not
 * the version looked up at the start of the run. If the two disagree -- the KEK
 * was rotated again mid-run, or a stale client wrapped under an older version --
 * rotation must stop rather than write a row that names a version which cannot
 * unwrap it, and must not spin re-selecting the same rows.
 *
 * <p>Demo profile: the KEK client is a spy over {@link MockKekClient}, whose
 * {@code rotateToNewVersion} stands in for a Key Vault key rotation.
 */
@SpringBootTest
@ActiveProfiles("demo")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class KekRotationVersionTest {

    @DynamicPropertySource
    static void overrideDatasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:mem:kekversion-" + System.nanoTime() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
    }

    @Autowired
    private RotationService rotationService;

    @Autowired
    private EdekRecordRepository edekRecordRepository;

    @MockitoSpyBean
    private KekClient kekClient;

    private record Seeded(UUID edekId, byte[] dek) {
    }

    private Seeded seed(String kekName) {
        byte[] dek = DekManager.generateDek();
        KekClient.WrapResult wrapResult = kekClient.wrapDek(dek, kekName);
        UUID edekId = UUID.randomUUID();
        edekRecordRepository.save(new EdekRecord(
                edekId, "kek-version-app", Base64.getEncoder().encodeToString(wrapResult.edekBytes()),
                wrapResult.kekVersion(), kekName, DekManager.ALGORITHM, "utf8", null, null, null));
        return new Seeded(edekId, dek);
    }

    private EdekRecord load(UUID edekId) {
        return edekRecordRepository.findById(edekId).orElseThrow();
    }

    /** The row's recorded (kek_name, kek_version) must unwrap its blob back to the original DEK. */
    private void assertUnwrapsWithRecordedVersion(Seeded seeded) {
        EdekRecord row = load(seeded.edekId());
        byte[] unwrapped = kekClient.unwrapDek(Base64.getDecoder().decode(row.getEdekBlob()),
                row.getKekName(), row.getKekVersion());
        assertArrayEquals(seeded.dek(), unwrapped, "row must unwrap with the version it records");
    }

    private MockKekClient demoKms() {
        return (MockKekClient) kekClient;
    }

    @Test
    void rotationRewrapsUnderTheNewVersionAndRowsStillUnwrap() {
        String kek = "kv-rotate-kek";
        Seeded a = seed(kek);
        Seeded b = seed(kek);
        String oldVersion = load(a.edekId()).getKekVersion();

        String newVersion = demoKms().rotateToNewVersion(kek);
        RotateKekResponse response = rotationService.rotateKek("test", kek);

        assertNotEquals(oldVersion, newVersion);
        assertEquals(2, response.recordsQueued());
        assertEquals(newVersion, load(a.edekId()).getKekVersion());
        assertEquals(newVersion, load(b.edekId()).getKekVersion());
        assertUnwrapsWithRecordedVersion(a);
        assertUnwrapsWithRecordedVersion(b);
    }

    /**
     * The production failure mode: rotation looks up version N+1 but the wrap
     * lands on N (a stale client, or N+2 because Key Vault rotated again).
     * Rotation must stop, leave the row as it was (still decryptable), and not loop.
     */
    @Test
    void rotationStopsWhenTheWrapLandsOnADifferentVersionThanTargeted() {
        String kek = "kv-midrun-kek";
        Seeded a = seed(kek);
        String before = load(a.edekId()).getKekVersion();
        String blobBefore = load(a.edekId()).getEdekBlob();

        // Lookup says a newer version exists; the wrap (real MockKekClient) still uses the current one.
        doReturn("demo-v-not-what-wrap-uses").when(kekClient).getCurrentKekVersion(kek);

        IllegalStateException e = assertTimeoutPreemptively(Duration.ofSeconds(30),
                () -> assertThrows(IllegalStateException.class, () -> rotationService.rotateKek("test", kek)));

        assertTrue(e.getMessage().contains("rerun the rotation"), e.getMessage());
        EdekRecord after = load(a.edekId());
        assertEquals(before, after.getKekVersion(), "row keeps its old version");
        assertEquals(blobBefore, after.getEdekBlob(), "row keeps its old blob");
        assertUnwrapsWithRecordedVersion(a);
    }

    @Test
    void rekeyRecordsTheVersionTheWrapUsedNotTheOneLookedUpAtStart() {
        String from = "kv-rekey-from";
        String to = "kv-rekey-to";
        Seeded a = seed(from);
        demoKms().rotateToNewVersion(to);
        String actual = kekClient.getCurrentKekVersion(to);

        // Simulate the target KEK rotating between the start-of-run lookup and the wraps.
        doReturn("demo-v-stale-lookup").when(kekClient).getCurrentKekVersion(to);

        rotationService.rekey(from, to, "test");

        EdekRecord row = load(a.edekId());
        assertEquals(to, row.getKekName());
        assertEquals(actual, row.getKekVersion());
        assertUnwrapsWithRecordedVersion(a);
    }
}
