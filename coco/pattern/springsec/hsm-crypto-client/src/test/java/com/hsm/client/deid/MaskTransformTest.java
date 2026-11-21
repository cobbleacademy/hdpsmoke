package com.hsm.client.deid;

import com.hsm.client.deid.transform.MaskTransform;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MaskTransformTest {

    private static String mask(Map<String, Object> options, String value) {
        return MaskTransform.create(new FieldOptions(options)).apply(value, Map.of(), new FieldStats());
    }

    @Test
    void keepLastShowsTheLastDigitsOfACard() {
        assertEquals("************8901", mask(Map.of("keep-last", 4), "6035712345678901"));
    }

    @Test
    void emailKeepsTheDomainAndTheFirstCharacter() {
        Map<String, Object> o = Map.of("keep", "domain", "keep-first", 1);
        assertEquals("j*******@mail.com", mask(o, "jane.doe@mail.com"));
        assertEquals("r****@corp.org", mask(o, "raj.p@corp.org"));
        assertEquals("**********", mask(o, "not-email!"), "no @: masked in full");
        assertEquals("a***@host.com", mask(o, "ab@x@host.com"), "the domain starts at the last @");
    }

    @Test
    void preserveLeavesSeparatorsInPlace() {
        assertEquals("****-****-****-8901",
                mask(Map.of("keep-last", 4, "preserve", List.of("-")), "6035-7123-4567-8901"));
        assertEquals("***-***-0142", mask(Map.of("keep-last", 4, "preserve", List.of("-")), "312-555-0142"));
    }

    @Test
    void noOptionsMasksEverything() {
        assertEquals("********", mask(Map.of(), "Jane Doe"));
    }

    @Test
    void fixedLengthHidesTheRealLength() {
        assertEquals("****", mask(Map.of("fixed-length", 4), "4821"));
        assertEquals("****", mask(Map.of("fixed-length", 4), "123456789"));
        assertEquals("********8901", mask(Map.of("fixed-length", 8, "keep-last", 4), "6035712345678901"));
    }

    @Test
    void aValueShorterThanTheKeptPartsIsMaskedInFull() {
        assertEquals("****", mask(Map.of("keep-first", 2, "keep-last", 2), "abcd"));
        assertEquals("***", mask(Map.of("keep-last", 4), "123"));
    }

    @Test
    void customCharacterAndUnicodeCodePoints() {
        assertEquals("##ë", mask(Map.of("char", "#", "keep-last", 1), "Zoë"));
        assertEquals("•••4", mask(Map.of("char", "•", "keep-last", 1), "1234"));
    }

    @Test
    void nullAndEmptyPassThrough() {
        assertNull(mask(Map.of("keep-last", 4), null));
        assertEquals("", mask(Map.of("keep-last", 4), ""));
    }

    @Test
    void badOptionsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> mask(Map.of("keep-lst", 4), "x"));
        assertThrows(IllegalArgumentException.class, () -> mask(Map.of("keep", "local"), "x"));
        assertThrows(IllegalArgumentException.class, () -> mask(Map.of("keep", "domain", "keep-last", 2), "x"));
        assertThrows(IllegalArgumentException.class, () -> mask(Map.of("char", "**"), "x"));
        assertThrows(IllegalArgumentException.class, () -> mask(Map.of("keep-last", -1), "x"));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> mask(Map.of("fixed-length", 4, "preserve", List.of("-")), "x"));
        assertTrue(e.getMessage().contains("fixed-length"));
    }
}
