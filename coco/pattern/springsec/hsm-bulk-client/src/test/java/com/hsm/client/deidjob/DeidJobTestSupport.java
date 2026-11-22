package com.hsm.client.deidjob;

import com.hsm.crypto.DekManager;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.crypto.AEADBadTagException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/** A fake hsm-core, an H2 database and the gift-card table for the DEID job tests. */
final class DeidJobTestSupport {

    static final Clock CLOCK_2026 = Clock.fixed(Instant.parse("2026-11-20T15:30:00Z"), ZoneOffset.UTC);
    static final UUID TOKEN_KEY_ID = UUID.fromString("6f1c0000-0000-4000-8000-000000000001");
    static final UUID SURROGATE_KEY_ID = UUID.fromString("6f1c0000-0000-4000-8000-000000000003");
    static final UUID COLUMN_DEK_ID = UUID.fromString("0c0c0000-0000-4000-8000-0000000000aa");
    static final String OWNER = "giftcard-svc";

    private DeidJobTestSupport() {
    }

    static byte[] dek(int seed) {
        byte[] k = new byte[32];
        for (int i = 0; i < k.length; i++) {
            k[i] = (byte) (seed * 31 + i);
        }
        return k;
    }

    /** hsm-core with three DEKs: the token key, the surrogate-table key and a column DEK. */
    static final class FakeHsm implements DeidJob.Hsm {
        final Map<UUID, byte[]> deks = new HashMap<>();
        final AtomicInteger unwraps = new AtomicInteger();
        boolean closed;

        FakeHsm() {
            deks.put(TOKEN_KEY_ID, dek(1));
            deks.put(SURROGATE_KEY_ID, dek(3));
            deks.put(COLUMN_DEK_ID, dek(9));
        }

        String encrypt(String plaintext) {
            DekManager.EncryptResult r = DekManager.encrypt(plaintext.getBytes(StandardCharsets.UTF_8), deks.get(COLUMN_DEK_ID), OWNER);
            return DekManager.packToken(COLUMN_DEK_ID, r.iv(), r.tag(), r.ciphertext());
        }

        @Override
        public byte[] unwrapDek(UUID edekId) {
            unwraps.incrementAndGet();
            byte[] k = deks.get(edekId);
            if (k == null) {
                throw new IllegalStateException("dek/unwrap failed: no decrypt grant on edek_id");
            }
            return k.clone();
        }

        @Override
        public String decryptToString(String token) {
            DekManager.UnpackedToken t = DekManager.unpackToken(token);
            byte[] k = deks.get(t.edekId());
            if (k == null) {
                throw new IllegalStateException("dek/unwrap failed: no decrypt grant on edek_id");
            }
            try {
                return new String(DekManager.decrypt(t.ciphertext(), t.tag(), t.iv(), k, OWNER), StandardCharsets.UTF_8);
            } catch (AEADBadTagException e) {
                throw new IllegalStateException("ciphertext authentication failed");
            }
        }

        @Override
        public byte[] decryptFile(InputStream in) {
            throw new UnsupportedOperationException("not used in these tests");
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static final AtomicInteger DB_SEQ = new AtomicInteger();

    /** A fresh in-memory H2 database in PostgreSQL mode. */
    static DriverManagerDataSource h2() {
        return new DriverManagerDataSource("jdbc:h2:mem:deid" + DB_SEQ.incrementAndGet()
                + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1", "sa", "");
    }

    /** Runs the shipped Postgres DDL. */
    static void createSurrogateTable(JdbcTemplate jdbc) {
        runDdl(jdbc, "deid/deid-surrogate-postgres.sql");
    }

    /** Runs a shipped DDL file: comments stripped, split on semicolons. */
    static void runDdl(JdbcTemplate jdbc, String resource) {
        try (InputStream in = DeidJobTestSupport.class.getResourceAsStream("/" + resource)) {
            String sql = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            StringBuilder stmt = new StringBuilder();
            for (String line : sql.split("\n")) {
                int c = line.indexOf("--");
                stmt.append(c >= 0 ? line.substring(0, c) : line).append('\n');
            }
            for (String s : stmt.toString().split(";")) {
                if (!s.isBlank()) {
                    jdbc.execute(s);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The gift-card table: two columns encrypted by the owning app. */
    static void createGiftcards(JdbcTemplate jdbc, FakeHsm hsm, boolean encrypted) {
        jdbc.execute("""
                CREATE TABLE giftcards (
                    id integer PRIMARY KEY, member_id varchar(20), first_name varchar(40), email varchar(400),
                    dob date, zip varchar(10), card_number varchar(400), card_pin varchar(8), amount numeric(10,2),
                    status varchar(20), issued_date timestamp)
                """);
        Object[][] rows = {
                {1, "M1234567", "Jane", "jane.doe@mail.com", "1985-03-14", "60614-2231", "6035712345678901", "4821", "25.00", "ACTIVE", "2026-11-02 10:15:00"},
                {2, "M7654321", "Raj", "raj.p@corp.org", "1936-07-01", "03601", "6035719876543210", "1122", "100.00", "REDEEMED", "2026-10-18 08:00:00"},
                {3, "M1234567", "Jane", "jane.doe@mail.com", "1985-03-14", "60614", "6035712345678901", "4821", "-5.00", "ADJUSTED", "2026-11-05 12:00:00"},
                {4, "M5550001", null, null, null, null, null, null, "10.00", "ACTIVE", null},
        };
        for (Object[] r : rows) {
            String email = (String) r[3];
            String card = (String) r[6];
            if (encrypted) {
                email = email == null ? null : hsm.encrypt(email);
                card = card == null ? null : hsm.encrypt(card);
            }
            jdbc.update("INSERT INTO giftcards VALUES (?, ?, ?, ?, CAST(? AS date), ?, ?, ?, CAST(? AS numeric(10,2)), ?, "
                    + "CAST(? AS timestamp))", r[0], r[1], r[2], email, r[4], r[5], card, r[7], r[8], r[9], r[10]);
        }
    }
}
