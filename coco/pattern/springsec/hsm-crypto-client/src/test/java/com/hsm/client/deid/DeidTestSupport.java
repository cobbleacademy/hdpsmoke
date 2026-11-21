package com.hsm.client.deid;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hsm.crypto.DekManager;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Shared fixtures: test keys, an in-memory surrogate table, ciphertext, profiles from YAML. */
final class DeidTestSupport {

    /** The run date of every test: 2026-11-20, so the birth-year cut-off is 1936. */
    static final Clock CLOCK_2026 = Clock.fixed(Instant.parse("2026-11-20T15:30:00Z"), ZoneOffset.UTC);

    /** The fixed DEK of deid-test-vectors.json: bytes 0x00..0x1f. */
    static final byte[] VECTOR_DEK = HexFormat.of().parseHex(
            "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f");

    static final UUID TOKEN_KEY_ID = UUID.fromString("6f1c0000-0000-4000-8000-000000000001");
    static final UUID OTHER_VENDOR_KEY_ID = UUID.fromString("6f1c0000-0000-4000-8000-000000000002");
    static final UUID SURROGATE_KEY_ID = UUID.fromString("6f1c0000-0000-4000-8000-000000000003");

    private DeidTestSupport() {
    }

    /** A DEK source that serves fixed DEKs and counts unwraps. */
    static final class FakeDekSource implements TokenKeyProvider.DekSource {
        final Map<UUID, byte[]> deks = new HashMap<>();
        final AtomicInteger unwraps = new AtomicInteger();

        FakeDekSource with(UUID id, byte[] dek) {
            deks.put(id, dek.clone());
            return this;
        }

        @Override
        public byte[] unwrap(UUID edekId) {
            unwraps.incrementAndGet();
            byte[] dek = deks.get(edekId);
            if (dek == null) {
                throw new IllegalStateException("dek/unwrap failed: app has no grant on this DEK");
            }
            return dek.clone();
        }
    }

    static byte[] dek(int seed) {
        byte[] d = new byte[32];
        for (int i = 0; i < d.length; i++) {
            d[i] = (byte) (seed * 31 + i);
        }
        return d;
    }

    static FakeDekSource standardKeys() {
        return new FakeDekSource()
                .with(TOKEN_KEY_ID, VECTOR_DEK)
                .with(OTHER_VENDOR_KEY_ID, dek(2))
                .with(SURROGATE_KEY_ID, dek(3));
    }

    /**
     * deid_surrogate in memory, enforcing both unique keys like the real table. Thread
     * safe, so concurrent runs can be tested.
     */
    static class InMemorySurrogateStore implements SurrogateStore {
        record Row(byte[] valueEnc, UUID keyEdekId, String surrogate, String runId) {
        }

        final Map<String, Row> rows = new ConcurrentHashMap<>();
        final Map<String, String> surrogatesTaken = new ConcurrentHashMap<>();

        private static String key(String scope, String domain, String hex) {
            return scope + "|" + domain + "|" + hex;
        }

        @Override
        public Map<String, String> find(String scope, String domain, List<byte[]> lookupHashes) {
            Map<String, String> out = new HashMap<>();
            for (byte[] h : lookupHashes) {
                String hex = HexFormat.of().formatHex(h);
                Row row = rows.get(key(scope, domain, hex));
                if (row != null) {
                    out.put(hex, row.surrogate());
                }
            }
            return out;
        }

        @Override
        public synchronized InsertResult insertIfAbsent(String scope, String domain, byte[] lookupHash, byte[] valueEnc,
                                                        UUID keyEdekId, String surrogate, String runId) {
            String k = key(scope, domain, HexFormat.of().formatHex(lookupHash));
            if (rows.containsKey(k)) {
                return InsertResult.ROW_EXISTS;
            }
            String sk = scope + "|" + domain + "|" + surrogate;
            if (surrogatesTaken.containsKey(sk)) {
                return InsertResult.SURROGATE_TAKEN;
            }
            rows.put(k, new Row(valueEnc, keyEdekId, surrogate, runId));
            surrogatesTaken.put(sk, k);
            return InsertResult.INSERTED;
        }

        Row rowFor(String surrogate) {
            return rows.values().stream().filter(r -> r.surrogate().equals(surrogate)).findFirst().orElseThrow();
        }
    }

    /**
     * Ciphertext as hsm-core or HsmCryptoClient writes it, under one of a few owner apps'
     * DEKs, and a decryptor that decrypts it locally, standing in for /dek/unwrap.
     */
    static final class FakeHsm {
        static final String OWNER = "giftcard-svc";
        final Map<UUID, byte[]> deks = new HashMap<>();
        final UUID columnDek = UUID.fromString("0c0c0000-0000-4000-8000-0000000000aa");

        FakeHsm() {
            deks.put(columnDek, dek(9));
        }

        String encrypt(String plaintext) {
            DekManager.EncryptResult r = DekManager.encrypt(plaintext.getBytes(StandardCharsets.UTF_8), deks.get(columnDek), OWNER);
            return DekManager.packToken(columnDek, r.iv(), r.tag(), r.ciphertext());
        }

        FieldDecryptor decryptor() {
            return new FieldDecryptor(token -> {
                DekManager.UnpackedToken t = DekManager.unpackToken(token);
                byte[] dek = deks.get(t.edekId());
                if (dek == null) {
                    throw new IllegalStateException("dek/unwrap failed: no decrypt grant on edek_id");
                }
                try {
                    return new String(DekManager.decrypt(t.ciphertext(), t.tag(), t.iv(), dek, OWNER), StandardCharsets.UTF_8);
                } catch (javax.crypto.AEADBadTagException e) {
                    throw new IllegalStateException("ciphertext authentication failed: tampered or corrupt");
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    static DeidProfile profile(String yaml) {
        Map<String, Object> doc = new Yaml().load(yaml);
        return DeidProfile.fromMap(doc);
    }

    static JsonNode vectors() {
        try (InputStream in = DeidTestSupport.class.getResourceAsStream("/deid-test-vectors.json")) {
            return new ObjectMapper().readTree(in);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static Map<String, String> record(String... keyValues) {
        Map<String, String> r = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            r.put(keyValues[i], keyValues[i + 1]);
        }
        return r;
    }
}
