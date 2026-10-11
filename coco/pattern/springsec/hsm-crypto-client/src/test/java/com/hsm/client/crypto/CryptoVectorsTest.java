package com.hsm.client.crypto;

import com.fasterxml.jackson.databind.JsonNode;
import com.hsm.client.config.FipsBootstrap;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import javax.crypto.AEADBadTagException;
import java.io.IOException;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * hsm-crypto-client's own DekManager/TransportWrapper against the shared,
 * language-neutral vectors in java/chassis/test-vectors/crypto-v1.json -- the same
 * file hsm-core-service, hsm-crypto-core and the Databricks UDF run. If this
 * fails, the change broke compatibility with existing ciphertexts: fix the code,
 * never the vectors. See java/docs/COMPATIBILITY_CONTRACT.md (C1-C4).
 */
class CryptoVectorsTest {

    static {
        FipsBootstrap.register();
    }

    private static final HexFormat HEX = HexFormat.of();
    private static final JsonNode VECTORS = load();

    private static JsonNode load() {
        try {
            return new ObjectMapper().readTree(Path.of("../chassis/test-vectors/crypto-v1.json").toFile());
        } catch (IOException e) {
            throw new IllegalStateException("crypto-v1.json not found relative to " + Path.of("").toAbsolutePath(), e);
        }
    }

    private static byte[] hex(JsonNode c, String field) {
        return HEX.parseHex(c.get(field).asText());
    }

    @TestFactory
    List<DynamicTest> validTokens() {
        List<DynamicTest> tests = new ArrayList<>();
        for (JsonNode c : VECTORS.at("/token/valid")) {
            String name = c.get("name").asText();
            tests.add(DynamicTest.dynamicTest("pack " + name, () -> assertEquals(c.get("token").asText(),
                    DekManager.packToken(UUID.fromString(c.get("edek_id").asText()), hex(c, "iv_hex"), hex(c, "tag_hex"), hex(c, "ciphertext_hex")))));
            for (String field : List.of("token", "token_unpadded")) {
                tests.add(DynamicTest.dynamicTest("unpack " + field + " " + name, () -> {
                    DekManager.UnpackedToken t = DekManager.unpackToken(c.get(field).asText());
                    assertEquals(UUID.fromString(c.get("edek_id").asText()), t.edekId());
                    assertArrayEquals(hex(c, "iv_hex"), t.iv());
                    assertArrayEquals(hex(c, "tag_hex"), t.tag());
                    assertArrayEquals(hex(c, "ciphertext_hex"), t.ciphertext());
                }));
            }
            tests.add(DynamicTest.dynamicTest("decrypt " + name, () -> assertArrayEquals(hex(c, "plaintext_hex"),
                    DekManager.decrypt(hex(c, "ciphertext_hex"), hex(c, "tag_hex"), hex(c, "iv_hex"), hex(c, "dek_hex"), c.get("app_id").asText()))));
            tests.add(DynamicTest.dynamicTest("fingerprint " + name, () -> assertEquals(c.get("fingerprint").asText(),
                    DekManager.makeFingerprint(hex(c, "iv_hex"), hex(c, "tag_hex")))));
        }
        return tests;
    }

    @TestFactory
    List<DynamicTest> invalidTokens() {
        List<DynamicTest> tests = new ArrayList<>();
        for (JsonNode c : VECTORS.at("/token/invalid_format")) {
            tests.add(DynamicTest.dynamicTest(c.get("name").asText(), () ->
                    assertThrows(IllegalArgumentException.class, () -> DekManager.unpackToken(c.get("token").asText()))));
        }
        for (JsonNode c : VECTORS.at("/token/decrypt_must_fail")) {
            tests.add(DynamicTest.dynamicTest(c.get("name").asText(), () -> {
                DekManager.UnpackedToken t = DekManager.unpackToken(c.get("token").asText());
                assertThrows(AEADBadTagException.class, () ->
                        DekManager.decrypt(t.ciphertext(), t.tag(), t.iv(), hex(c, "dek_hex"), c.get("app_id").asText()));
            }));
        }
        return tests;
    }

    @TestFactory
    List<DynamicTest> transport() {
        JsonNode t = VECTORS.get("transport");
        PrivateKey privateKey = TransportWrapper.parsePrivateKeyPem(t.get("private_key_pem").asText());
        PublicKey publicKey = TransportWrapper.parsePublicKeyPem(t.get("public_key_pem").asText());
        List<DynamicTest> tests = new ArrayList<>();
        for (JsonNode c : t.get("cases")) {
            byte[] dek = hex(c, "dek_hex");
            tests.add(DynamicTest.dynamicTest("unwrap " + c.get("name").asText(), () -> assertArrayEquals(dek,
                    TransportWrapper.unwrap(Base64.getDecoder().decode(c.get("wrapped_b64").asText()), privateKey))));
            tests.add(DynamicTest.dynamicTest("wrap round trip " + c.get("name").asText(), () -> assertArrayEquals(dek,
                    TransportWrapper.unwrap(TransportWrapper.wrap(dek, publicKey), privateKey))));
        }
        return tests;
    }
}
