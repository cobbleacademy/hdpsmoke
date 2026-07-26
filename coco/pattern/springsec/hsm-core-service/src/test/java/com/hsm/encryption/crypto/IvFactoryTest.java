package com.hsm.encryption.crypto;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IvFactoryTest {

    @BeforeAll
    static void registerProvider() {
        TestFipsSupport.ensureReady();
    }

    @Test
    void generatesTwelveBytes() {
        assertEquals(12, IvFactory.generate().length);
    }

    @Test
    void successiveCallsAreDistinct() {
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            seen.add(new String(IvFactory.generate(), java.nio.charset.StandardCharsets.ISO_8859_1));
        }
        assertTrue(seen.size() > 95, "expected near-100% uniqueness across 100 draws, got " + seen.size());
    }
}
