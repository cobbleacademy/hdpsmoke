package com.hsm.client.deid;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hsm.client.deid.DeidTestSupport.CLOCK_2026;
import static com.hsm.client.deid.DeidTestSupport.SURROGATE_KEY_ID;
import static com.hsm.client.deid.DeidTestSupport.profile;
import static com.hsm.client.deid.DeidTestSupport.record;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The worked example: a gift-card table with two encrypted columns, exported for a
 * fulfillment vendor (minimum-necessary, masked) and a reporting vendor (Safe Harbor).
 */
class DeidPipelineTest {

    private static final List<String> SOURCE_COLUMNS = List.of("member_id", "first_name", "last_name", "email", "phone",
            "dob", "zip", "card_number", "card_pin", "program", "card_type", "amount", "status", "issued_date");

    private static final String ACME = """
            id: acme-fulfillment
            version: 1
            vendor: acme-rewards
            hipaa-basis: minimum-necessary
            token-key-edek-id: 6f1c0000-0000-4000-8000-000000000001
            token-domain: member
            fields:
              - { name: member_id,   phi: member-id,   method: tokenize }
              - { name: first_name,  phi: name,        method: passthrough }
              - { name: last_name,   phi: name,        method: passthrough }
              - { name: email,       phi: email,       decrypt: true, method: mask, keep: domain, keep-first: 1 }
              - { name: phone,       phi: phone,       method: mask, keep-last: 4, preserve: ["-"] }
              - { name: card_number, phi: card-number, decrypt: true, method: mask, keep-last: 4 }
              - { name: card_pin,    method: drop }
              - { name: card_type,   method: passthrough }
              - { name: amount,      method: passthrough }
              - { name: status,      method: passthrough }
            output: { file: "giftcards_acme_{yyyyMMdd}.csv" }
            """;

    private static final String ZEN = """
            id: zen-reporting
            version: 1
            vendor: zen-analytics
            hipaa-basis: safe-harbor
            surrogate-scope: zen-analytics
            fields:
              - { name: member_id,   phi: member-id,   method: surrogate, domain: member }
              - { name: card_number, phi: card-number, decrypt: true, method: surrogate, domain: card, normalize: digits }
              - { name: dob,         phi: birth-date,  method: generalize, to: birth-year }
              - { name: zip,         phi: zip,         method: generalize, to: zip3 }
              - { name: issued_date, phi: date,        method: generalize, to: year }
              - { name: card_type,   method: passthrough }
              - { name: amount,      method: passthrough }
              - { name: status,      method: passthrough }
            output:
              file: "giftcards_zen_{yyyyMMdd}.csv"
              rename: { member_id: member_ref, card_number: card_ref, dob: birth_year, zip: zip3, issued_date: issued_year }
            """;

    private final DeidTestSupport.FakeHsm hsm = new DeidTestSupport.FakeHsm();
    private final TokenKeyProvider keys = new TokenKeyProvider(DeidTestSupport.standardKeys());
    private final DeidTestSupport.InMemorySurrogateStore store = new DeidTestSupport.InMemorySurrogateStore();

    private List<Map<String, String>> table() {
        List<Map<String, String>> rows = new ArrayList<>();
        rows.add(record("member_id", "M1234567", "first_name", "Jane", "last_name", "Doe",
                "email", hsm.encrypt("jane.doe@mail.com"), "phone", "312-555-0142", "dob", "1958-03-12", "zip", "60614",
                "card_number", hsm.encrypt("6035712345678901"), "card_pin", "4821", "program", "diabetes-screening",
                "card_type", "VISA", "amount", "50.00", "status", "issued", "issued_date", "2026-10-02"));
        rows.add(record("member_id", "M1234567", "first_name", "Jane", "last_name", "Doe",
                "email", hsm.encrypt("jane.doe@mail.com"), "phone", "312-555-0142", "dob", "1958-03-12", "zip", "60614",
                "card_number", hsm.encrypt("6035719876543210"), "card_pin", "1190", "program", "flu-shot",
                "card_type", "AMAZON", "amount", "25.00", "status", "redeemed", "issued_date", "2026-11-05"));
        rows.add(record("member_id", "M7654321", "first_name", "Raj", "last_name", "Patel",
                "email", hsm.encrypt("raj.p@corp.org"), "phone", "617-555-0199", "dob", "1933-07-30", "zip", "03601",
                "card_number", hsm.encrypt("6035714444333322"), "card_pin", "7730", "program", "annual-wellness",
                "card_type", "VISA", "amount", "100.00", "status", "issued", "issued_date", "2026-11-12"));
        rows.add(record("member_id", "M5550001", "first_name", "Ana", "last_name", "Lopez",
                "email", hsm.encrypt("a.lopez@web.net"), "phone", "415-555-0110", "dob", "1990-01-05", "zip", "94107",
                "card_number", hsm.encrypt("6035711111222233"), "card_pin", "3307", "program", "smoking-cessation",
                "card_type", "TARGET", "amount", "75.00", "status", "expired", "issued_date", "2025-12-20"));
        return rows;
    }

    private DeidPipeline pipeline(DeidProfile p) {
        ProfileValidator.validate(p, SOURCE_COLUMNS);
        TransformContext ctx = new TransformContext(CLOCK_2026, "deid-test-run", keys, SURROGATE_KEY_ID, store, null);
        return new DeidPipeline(p, new TransformRegistry(p, ctx), hsm.decryptor());
    }

    @Test
    void fulfillmentFileIsMaskedAndTokenized() {
        DeidPipeline acme = pipeline(profile(ACME));
        assertEquals(List.of("member_id", "first_name", "last_name", "email", "phone", "card_number", "card_type",
                "amount", "status"), acme.header());

        List<List<String>> out = acme.process(table(), 1);

        assertEquals(List.of("Jane", "Doe", "j*******@mail.com", "***-***-0142", "************8901", "VISA", "50.00",
                "issued"), out.get(0).subList(1, 9));
        assertEquals("************3210", out.get(1).get(5));
        assertEquals("r****@corp.org", out.get(2).get(3));
        assertEquals("a******@web.net", out.get(3).get(3));
        assertTrue(out.get(0).get(0).matches("tk_[A-Za-z0-9_-]{22}"));
        assertEquals(out.get(0).get(0), out.get(1).get(0), "Jane's two cards share one token");
        assertNotEquals(out.get(0).get(0), out.get(2).get(0));
    }

    @Test
    void reportingFileIsSafeHarbor() {
        DeidPipeline zen = pipeline(profile(ZEN));
        assertEquals(List.of("member_ref", "card_ref", "birth_year", "zip3", "issued_year", "card_type", "amount",
                "status"), zen.header());

        List<List<String>> out = zen.process(table(), 1);

        assertEquals(List.of("1958", "606", "2026", "VISA", "50.00", "issued"), out.get(0).subList(2, 8));
        assertEquals(List.of("<=1936", "000", "2026"), out.get(2).subList(2, 5), "Raj is 93; ZIP3 036 is restricted");
        assertEquals(List.of("1990", "941", "2025"), out.get(3).subList(2, 5));
        assertEquals(out.get(0).get(0), out.get(1).get(0), "same member, same surrogate");
        assertNotEquals(out.get(0).get(1), out.get(1).get(1), "different cards, different surrogates");
        out.forEach(row -> assertTrue(row.get(0).matches("S-[0-9A-Z]{5}-[0-9A-Z]{5}"), row.get(0)));

        // Next month's run reuses the surrogates.
        List<List<String>> nextRun = pipeline(profile(ZEN)).process(table(), 1);
        assertEquals(out.get(2).get(0), nextRun.get(2).get(0));
        assertEquals(out.get(2).get(1), nextRun.get(2).get(1));
    }

    @Test
    void unlistedAndDroppedFieldsNeverReachTheOutput() {
        DeidPipeline acme = pipeline(profile(ACME));
        String all = acme.process(table(), 1).toString() + acme.header();
        for (String secret : List.of("M1234567", "4821", "diabetes", "1958", "60614", "6035712345678901", "jane.doe",
                "card_pin", "program", "dob")) {
            assertFalse(all.contains(secret), secret + " leaked into " + all);
        }
        assertEquals(List.of("dob", "zip", "program", "issued_date"), acme.droppedByDefault(SOURCE_COLUMNS));
        assertEquals(List.of("first_name", "last_name", "card_type", "amount", "status"), acme.passthroughFields());
    }

    @Test
    void theReportCountsWithoutValues() {
        DeidPipeline zen = pipeline(profile(ZEN));
        zen.process(table(), 1);

        assertEquals(4, zen.rows());
        assertEquals(Set.of(hsm.columnDek), zen.decryptedEdekIds());
        List<DeidPipeline.FieldReport> report = zen.report();
        DeidPipeline.FieldReport card = report.get(1);
        assertEquals("card_number", card.source());
        assertEquals("card_ref", card.output());
        assertEquals("card-number", card.phi());
        assertEquals("surrogate@v1", card.method());
        assertTrue(card.decrypted());
        assertEquals(4L, card.counters().get("decrypted"));
        assertEquals(4L, card.counters().get("new"));
        DeidPipeline.FieldReport dob = report.get(2);
        assertEquals("birth-year", dob.settings().get("to"));
        assertEquals(1936, dob.settings().get("cutoff_year"));
        assertEquals(1L, dob.counters().get("capped"));
        assertFalse(report.toString().contains("M1234567"));
        assertFalse(report.toString().contains("S-"), "no surrogates in the report");
    }

    @Test
    void aBadValueNamesTheFieldAndRowButNotTheValue() {
        DeidPipeline zen = pipeline(profile(ZEN));
        List<Map<String, String>> rows = table();
        rows.get(2).put("dob", "30/07/1933");
        DeidException e = assertThrows(DeidException.class, () -> zen.process(rows, 101));
        assertEquals("dob", e.field());
        assertEquals(103, e.row());
        assertTrue(e.getMessage().startsWith("field 'dob', row 103: "), e.getMessage());
        assertFalse(e.getMessage().contains("1933"));
    }

    @Test
    void plainTextInAnEncryptedColumnFailsTheRow() {
        DeidPipeline acme = pipeline(profile(ACME));
        List<Map<String, String>> rows = table();
        rows.get(1).put("card_number", "6035719876543210");
        DeidException e = assertThrows(DeidException.class, () -> acme.process(rows, 1));
        assertEquals("card_number", e.field());
        assertEquals(2, e.row());
        assertFalse(e.getMessage().contains("6035719876543210"));
    }

    @Test
    void verifyDecryptCatchesAMissingGrantBeforeAnyOutput() {
        DeidPipeline acme = pipeline(profile(ACME));
        List<Map<String, String>> sample = table();
        hsm.deks.clear(); // as if the export's app_id had no decrypt grant
        DeidException e = assertThrows(DeidException.class, () -> acme.verifyDecrypt(sample));
        assertEquals("email", e.field());
        assertTrue(e.getMessage().contains("grant"));
    }

    @Test
    void aMaskOnlyProfileNeedsNoKeysTableOrDecryptor() {
        DeidProfile p = profile("""
                id: masked-only
                version: 1
                hipaa-basis: minimum-necessary
                fields:
                  - { name: phone,  method: mask, keep-last: 4 }
                  - { name: dob,    method: generalize, to: year }
                  - { name: amount, method: passthrough }
                output: { file: "out.csv" }
                """);
        assertFalse(DeidPipeline.needsHsm(p));
        assertFalse(DeidPipeline.needsSurrogateTable(p));
        DeidPipeline pipeline = new DeidPipeline(p,
                new TransformRegistry(p, TransformContext.withoutKeys(CLOCK_2026, "run")), null);
        assertEquals(List.of("********0142", "1958", "50.00"), pipeline.process(table(), 1).get(0));

        assertTrue(DeidPipeline.needsHsm(profile(ACME)));
        assertTrue(DeidPipeline.needsSurrogateTable(profile(ZEN)));
    }

    @Test
    void aRunMissingWhatAFieldNeedsFailsAtStart() {
        DeidProfile zen = profile(ZEN);
        IllegalStateException noTable = assertThrows(IllegalStateException.class, () -> new DeidPipeline(zen,
                new TransformRegistry(zen, new TransformContext(CLOCK_2026, "run", keys, SURROGATE_KEY_ID, null, null)),
                hsm.decryptor()));
        assertTrue(noTable.getMessage().contains("surrogate table"));

        IllegalStateException noDecryptor = assertThrows(IllegalStateException.class, () -> new DeidPipeline(zen,
                new TransformRegistry(zen, new TransformContext(CLOCK_2026, "run", keys, SURROGATE_KEY_ID, store, null)),
                null));
        assertTrue(noDecryptor.getMessage().contains("decryptor"));

        DeidProfile acme = profile(ACME);
        IllegalStateException noKeys = assertThrows(IllegalStateException.class, () -> new DeidPipeline(acme,
                new TransformRegistry(acme, TransformContext.withoutKeys(CLOCK_2026, "run")), hsm.decryptor()));
        assertTrue(noKeys.getMessage().contains("keys"));
    }
}
