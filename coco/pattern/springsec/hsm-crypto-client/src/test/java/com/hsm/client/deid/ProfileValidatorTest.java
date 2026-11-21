package com.hsm.client.deid;

import org.junit.jupiter.api.Test;

import java.util.List;

import static com.hsm.client.deid.DeidTestSupport.profile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProfileValidatorTest {

    private static final List<String> SOURCE = List.of("member_id", "first_name", "email", "phone", "dob", "zip",
            "card_number", "card_pin", "program", "card_type", "amount", "status", "issued_date", "age", "shipping_zip");

    private static String header(String basis) {
        return """
                id: test
                version: 1
                hipaa-basis: %s
                token-key-edek-id: 6f1c0000-0000-4000-8000-000000000001
                token-domain: member
                surrogate-scope: test-vendor
                output: { file: "out.csv" }
                fields:
                """.formatted(basis);
    }

    private static List<String> problems(String basis, String fields) {
        return ProfileValidator.problems(profile(header(basis) + fields), SOURCE);
    }

    private static void assertProblem(List<String> problems, String fragment) {
        assertTrue(problems.stream().anyMatch(p -> p.contains(fragment)), fragment + " not in " + problems);
    }

    @Test
    void aGoodSafeHarborProfilePasses() {
        assertEquals(List.of(), problems("safe-harbor", """
                  - { name: member_id,   phi: member-id,   method: surrogate, domain: member }
                  - { name: card_number, phi: card-number, method: surrogate, domain: card, decrypt: true }
                  - { name: dob,         phi: birth-date,  method: generalize, to: birth-year }
                  - { name: issued_date, phi: date,        method: generalize, to: year }
                  - { name: zip,         phi: zip,         method: generalize, to: zip3 }
                  - { name: age,         phi: age,         method: generalize, to: age-band, width: 10 }
                  - { name: email,       phi: email,       method: drop }
                  - { name: amount,                        method: passthrough }
                """));
    }

    @Test
    void safeHarborRejectsEachDisallowedMethod() {
        List<String> p = problems("safe-harbor", """
                  - { name: member_id,   phi: member-id,  method: tokenize }
                  - { name: email,       phi: email,      method: mask, keep: domain }
                  - { name: first_name,  phi: name,       method: passthrough }
                  - { name: dob,         phi: birth-date, method: generalize, to: year }
                  - { name: issued_date, phi: date,       method: passthrough }
                  - { name: zip,         phi: zip,        method: mask, keep-first: 3 }
                  - { name: age,         phi: age,        method: passthrough }
                """);
        assertProblem(p, "field 'member_id' (phi: member-id): safe-harbor allows only drop or surrogate (a token is derived");
        assertProblem(p, "field 'email' (phi: email): safe-harbor allows only drop");
        assertProblem(p, "field 'first_name' (phi: name): safe-harbor allows only drop");
        assertProblem(p, "field 'dob' (phi: birth-date): safe-harbor allows only drop or generalize to: birth-year");
        assertProblem(p, "field 'issued_date' (phi: date)");
        assertProblem(p, "field 'zip' (phi: zip)");
        assertProblem(p, "field 'age' (phi: age)");
        assertEquals(7, p.size(), p.toString());
    }

    @Test
    void safeHarborRejectsBirthYearAsOf() {
        assertProblem(problems("safe-harbor", """
                  - { name: dob, phi: birth-date, method: generalize, to: birth-year, as-of: issued_date }
                """), "safe-harbor doesn't allow as-of");
    }

    @Test
    void minimumNecessaryAllowsMaskingIdentifiers() {
        assertEquals(List.of(), problems("minimum-necessary", """
                  - { name: member_id,   phi: member-id,   method: tokenize }
                  - { name: first_name,  phi: name,        method: passthrough }
                  - { name: email,       phi: email,       method: mask, keep: domain, keep-first: 1, decrypt: true }
                  - { name: card_number, phi: card-number, method: mask, keep-last: 4, decrypt: true }
                """));
    }

    @Test
    void neverExportFieldsMayOnlyBeDropped() {
        List<String> p = problems("minimum-necessary", """
                  - { name: card_pin, method: mask }
                  - { name: amount,   method: passthrough }
                """);
        assertProblem(p, "field 'card_pin' is a never-export field");

        assertEquals(List.of(), problems("minimum-necessary", """
                  - { name: card_pin,     method: drop }
                  - { name: shipping_zip, method: passthrough }
                """), "shipping_zip contains 'pin' but isn't a PIN");
    }

    @Test
    void anOutputNameThatLooksLikeAPinIsRejected() {
        String yaml = header("minimum-necessary").replace("output: { file: \"out.csv\" }",
                "output: { file: \"out.csv\", rename: { amount: card_pin } }") + """
                  - { name: amount, method: passthrough }
                """;
        assertProblem(ProfileValidator.problems(profile(yaml), SOURCE), "looks like a never-export field");
    }

    @Test
    void decryptWithDropIsRejected() {
        assertProblem(problems("minimum-necessary", """
                  - { name: card_number, method: drop, decrypt: true }
                  - { name: amount,      method: passthrough }
                """), "decrypt: true with drop");
    }

    @Test
    void sourceAndStructureChecks() {
        List<String> p = problems("minimum-necessary", """
                  - { name: amount,   method: passthrough }
                  - { name: amount,   method: passthrough }
                  - { name: missing,  method: passthrough }
                  - { name: dob,      method: generalize, to: birth-year, as-of: no_such_field }
                  - { name: phone,    method: mask, keep-lst: 4 }
                """);
        assertProblem(p, "field 'amount' is listed more than once");
        assertProblem(p, "field 'missing' is not in the source");
        assertProblem(p, "as-of field 'no_such_field' is not in the source");
        assertProblem(p, "field 'phone': unknown option(s) for mask: keep-lst");
    }

    @Test
    void keysAndScopeAreRequiredForTheirMethods() {
        String yaml = """
                id: test
                version: 1
                hipaa-basis: minimum-necessary
                output: { file: "out.csv" }
                fields:
                  - { name: member_id,   method: tokenize }
                  - { name: card_number, method: surrogate, domain: card }
                """;
        List<String> p = ProfileValidator.problems(profile(yaml), SOURCE);
        assertProblem(p, "tokenize needs token-key-edek-id");
        assertProblem(p, "tokenize needs a domain");
        assertProblem(p, "surrogate needs surrogate-scope");
    }

    @Test
    void expertDeterminationNeedsAReport() {
        assertProblem(problems("expert-determination", """
                  - { name: amount, method: passthrough }
                """), "expert-report");
    }

    @Test
    void structuralProblemsAreAllReportedAtOnce() {
        DeidValidationException e = assertThrows(DeidValidationException.class, () -> profile("""
                id: x
                hipaa-basis: safe-habor
                colour: blue
                fields:
                  - { name: a, method: scramble }
                  - { method: drop }
                output: { file: out.csv, line-ending: cr }
                """));
        List<String> p = e.problems();
        assertProblem(p, "version is required");
        assertProblem(p, "unknown hipaa-basis 'safe-habor'");
        assertProblem(p, "unknown key(s) in profile: colour");
        assertProblem(p, "unknown method 'scramble'");
        assertProblem(p, "name is required");
        assertProblem(p, "output.line-ending must be lf or crlf");
    }

    @Test
    void validateThrowsWithEveryProblem() {
        DeidValidationException e = assertThrows(DeidValidationException.class,
                () -> ProfileValidator.validate(profile(header("safe-harbor") + """
                          - { name: first_name, phi: name, method: passthrough }
                          - { name: email,      phi: email, method: passthrough }
                        """), SOURCE));
        assertEquals(2, e.problems().size());
    }
}
