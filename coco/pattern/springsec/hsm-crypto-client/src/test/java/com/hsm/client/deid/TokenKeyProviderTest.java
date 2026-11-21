package com.hsm.client.deid;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.UUID;

import static com.hsm.client.deid.DeidTestSupport.SURROGATE_KEY_ID;
import static com.hsm.client.deid.DeidTestSupport.TOKEN_KEY_ID;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TokenKeyProviderTest {

    private static final HexFormat HEX = HexFormat.of();

    @Test
    void hkdfMatchesRfc5869TestCase3() {
        JsonNode c = DeidTestSupport.vectors().get("rfc5869_case3");
        byte[] okm = TokenKeyProvider.hkdfSha256(HEX.parseHex(c.get("ikm_hex").asText()), null, new byte[0],
                c.get("length").asInt());
        // The published RFC 5869 A.3 output, independent of this code base.
        assertEquals("8da4e775a563c18f715f802a063c5a31b8a11f5c5ee1879ec3454e5f3c738d2d9d201395faa4b61a96c8",
                HEX.formatHex(okm));
        assertEquals(c.get("okm_hex").asText(), HEX.formatHex(okm));
    }

    @Test
    void purposeKeysMatchTheTestVectors() {
        JsonNode v = DeidTestSupport.vectors();
        byte[] dek = HEX.parseHex(v.get("dek_hex").asText());
        for (JsonNode h : v.get("hkdf")) {
            byte[] key = TokenKeyProvider.hkdfSha256(dek, null, h.get("label").asText().getBytes(StandardCharsets.UTF_8), 32);
            assertEquals(h.get("key_hex").asText(), HEX.formatHex(key), h.get("label").asText());
        }
    }

    @Test
    void severalKeysFromOneDekCostOneUnwrapAndAreCached() {
        DeidTestSupport.FakeDekSource source = DeidTestSupport.standardKeys();
        try (TokenKeyProvider keys = new TokenKeyProvider(source)) {
            byte[][] pair = keys.keys(SURROGATE_KEY_ID, TokenKeyProvider.SURROGATE_LOOKUP_LABEL,
                    TokenKeyProvider.SURROGATE_ENC_LABEL);
            assertEquals(1, source.unwraps.get());
            assertFalse(java.util.Arrays.equals(pair[0], pair[1]), "different labels give different keys");

            assertSame(pair[0], keys.key(SURROGATE_KEY_ID, TokenKeyProvider.SURROGATE_LOOKUP_LABEL));
            assertEquals(1, source.unwraps.get(), "cached, no second unwrap");
        }
    }

    @Test
    void closeZeroesTheDerivedKeys() {
        TokenKeyProvider keys = new TokenKeyProvider(DeidTestSupport.standardKeys());
        byte[] key = keys.key(TOKEN_KEY_ID, TokenKeyProvider.TOKENIZE_LABEL);
        keys.close();
        assertArrayEquals(new byte[32], key);
        assertThrows(IllegalStateException.class, () -> keys.key(TOKEN_KEY_ID, TokenKeyProvider.TOKENIZE_LABEL));
    }

    @Test
    void anUnwrapFailureNamesTheKeyButNoKeyMaterial() {
        try (TokenKeyProvider keys = new TokenKeyProvider(DeidTestSupport.standardKeys())) {
            UUID unknown = UUID.randomUUID();
            DeidException e = assertThrows(DeidException.class, () -> keys.key(unknown, TokenKeyProvider.TOKENIZE_LABEL));
            assertTrue(e.getMessage().contains(unknown.toString()));
            assertTrue(e.getMessage().contains("no grant"));
        }
    }
}
