package com.hsm.client.deid;

import com.hsm.client.HsmCryptoClient;
import com.hsm.crypto.DekManager;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Decrypts the values of {@code decrypt: true} fields: columns that hold HSM ciphertext
 * (the {@code v1.…} token from hsm-core's {@code /encrypt} or
 * {@link HsmCryptoClient#encrypt}) rather than the value (DEIDENTIFICATION.md
 * section 6.3). The field's method then sees the plaintext.
 *
 * <ul>
 *   <li>Each token carries its own {@code edek_id}; the client's decrypt cache makes one
 *       {@code /dek/unwrap} per {@code edek_id} per TTL, not one per row.</li>
 *   <li>Null and empty values pass through untouched.</li>
 *   <li>A value that isn't a token, or fails to decrypt (tampered, no grant, KEK version
 *       disabled), throws {@link DeidException}: the run fails, naming the field and row
 *       but never the value.</li>
 *   <li>Plaintext is returned as a {@code String}, which Java can't zero; it lives one
 *       page at a time and is never logged or written anywhere except as the method's
 *       output.</li>
 * </ul>
 */
public final class FieldDecryptor {

    /** Decrypts one ciphertext token to UTF-8 text. */
    @FunctionalInterface
    public interface Decryptor {
        String decrypt(String token);
    }

    private static final String TOKEN_PREFIX = "v1.";

    private final Decryptor decryptor;
    private final Set<UUID> edekIds = ConcurrentHashMap.newKeySet();

    public FieldDecryptor(Decryptor decryptor) {
        this.decryptor = decryptor;
    }

    /** Decrypts through the client's {@code /dek/unwrap} and decrypt cache. */
    public static FieldDecryptor using(HsmCryptoClient client) {
        return new FieldDecryptor(client::decryptToString);
    }

    public String decrypt(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        String token = value.strip();
        DekManager.UnpackedToken unpacked;
        try {
            unpacked = DekManager.unpackToken(token);
        } catch (IllegalArgumentException e) {
            throw DeidException.badValue("marked decrypt: true but not an HSM ciphertext (" + e.getMessage() + ")");
        }
        edekIds.add(unpacked.edekId());
        try {
            return decryptor.decrypt(token);
        } catch (RuntimeException e) {
            throw DeidException.badValue("decrypt failed for edek_id=" + unpacked.edekId() + ": " + e.getMessage(), e);
        }
    }

    /**
     * Does this value look like HSM ciphertext? The dry run flags a column whose values
     * do but that isn't marked {@code decrypt: true}: sending ciphertext is useless to
     * the vendor and shows our {@code edek_id}s.
     */
    public static boolean looksEncrypted(String value) {
        if (value == null || !value.strip().startsWith(TOKEN_PREFIX)) {
            return false;
        }
        try {
            DekManager.unpackToken(value.strip());
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /** The distinct {@code edek_id}s seen so far, for the manifest. */
    public Set<UUID> edekIdsSeen() {
        return Collections.unmodifiableSet(edekIds);
    }
}
