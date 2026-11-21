package com.hsm.client.deid;

import com.fasterxml.jackson.databind.JsonNode;
import com.hsm.client.deid.transform.TokenizeTransform;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.hsm.client.deid.DeidTestSupport.OTHER_VENDOR_KEY_ID;
import static com.hsm.client.deid.DeidTestSupport.TOKEN_KEY_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TokenizeTransformTest {

    private final TokenKeyProvider keys = new TokenKeyProvider(DeidTestSupport.standardKeys());

    private TokenizeTransform tokenize(String domain, Map<String, Object> options, java.util.UUID keyId) {
        return TokenizeTransform.create(new FieldOptions(options), keys.key(keyId, TokenKeyProvider.TOKENIZE_LABEL), domain);
    }

    private String run(TokenizeTransform t, String value) {
        return t.apply(value, Map.of(), new FieldStats());
    }

    @Test
    void tokensMatchTheIndependentTestVectors() {
        JsonNode cases = DeidTestSupport.vectors().get("tokenize");
        assertTrue(cases.size() > 5);
        for (JsonNode c : cases) {
            TokenizeTransform t = tokenize(c.get("domain").asText(),
                    Map.of("normalize", c.get("normalize").asText()), TOKEN_KEY_ID);
            assertEquals(c.get("token").asText(), run(t, c.get("value").asText()),
                    c.get("domain").asText() + " / " + c.get("normalize").asText());
        }
    }

    @Test
    void formatIsPrefixPlus22Characters() {
        String token = run(tokenize("member", Map.of(), TOKEN_KEY_ID), "M1234567");
        assertTrue(token.matches("tk_[A-Za-z0-9_-]{22}"), token);
        String custom = run(tokenize("member", Map.of("prefix", "acme_"), TOKEN_KEY_ID), "M1234567");
        assertTrue(custom.startsWith("acme_"));
    }

    @Test
    void differentVendorKeysGiveDifferentTokens() {
        assertNotEquals(run(tokenize("member", Map.of(), TOKEN_KEY_ID), "M1234567"),
                run(tokenize("member", Map.of(), OTHER_VENDOR_KEY_ID), "M1234567"));
    }

    @Test
    void nullAndEmptyPassThrough() {
        TokenizeTransform t = tokenize("member", Map.of(), TOKEN_KEY_ID);
        assertNull(run(t, null));
        assertEquals("", run(t, ""));
    }

    @Test
    void aValueWithNothingLeftAfterNormalizeFailsWithoutTheValue() {
        TokenizeTransform t = tokenize("card", Map.of("normalize", "digits"), TOKEN_KEY_ID);
        DeidException e = assertThrows(DeidException.class, () -> run(t, "no-digits-here"));
        assertTrue(!e.getMessage().contains("no-digits-here"), e.getMessage());
    }

    @Test
    void unknownOptionsAreRejected() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> tokenize("member", Map.of("normalise", "lower"), TOKEN_KEY_ID));
        assertTrue(e.getMessage().contains("normalise"));
    }
}
