package com.hsm.client.deid;

import com.hsm.crypto.DekManager;
import com.hsm.crypto.FipsBootstrap;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * The keys behind {@code tokenize} and {@code surrogate} (DEIDENTIFICATION.md section 7).
 * Each is a named DEK issued by hsm-core and pinned by {@code edek_id}. For each one
 * needed, this:
 * <ol>
 *   <li>unwraps it once through {@link DekSource} ({@code /dek/unwrap});</li>
 *   <li>derives each purpose key with {@code HKDF-SHA256(DEK, salt = none, info = label, L = 32)};</li>
 *   <li>zeroes the DEK.</li>
 * </ol>
 * Derived keys are kept until {@link #close}, which zeroes them. Callers get the cached
 * array itself and must not change it. HMAC and HKDF run in BC-FIPS.
 */
public final class TokenKeyProvider implements AutoCloseable {

    /** HKDF label of the tokenize key. */
    public static final String TOKENIZE_LABEL = "hsm-deid-tokenize-v1";
    /** HKDF label of the key that computes surrogate-table lookup hashes. */
    public static final String SURROGATE_LOOKUP_LABEL = "hsm-deid-surrogate-lookup-v1";
    /** HKDF label of the key that encrypts surrogate-table values. */
    public static final String SURROGATE_ENC_LABEL = "hsm-deid-surrogate-enc-v1";

    private static final int KEY_LENGTH = 32;
    private static final int HASH_LENGTH = 32;
    private static final String HMAC = "HmacSHA256";

    static {
        FipsBootstrap.register();
    }

    /**
     * Unwraps a DEK by {@code edek_id}. Returns a fresh copy, which the provider zeroes
     * after deriving. {@code HsmCryptoClient::unwrapDek} is the production source.
     */
    @FunctionalInterface
    public interface DekSource {
        byte[] unwrap(UUID edekId);
    }

    private final DekSource source;
    private final Map<String, byte[]> derived = new HashMap<>();
    private boolean closed;

    public TokenKeyProvider(DekSource source) {
        this.source = source;
    }

    /** The purpose key for {@code label} under DEK {@code edekId}. */
    public byte[] key(UUID edekId, String label) {
        return keys(edekId, label)[0];
    }

    /**
     * Several purpose keys from one DEK, with at most one unwrap. Returned in the order
     * of {@code labels}.
     */
    public synchronized byte[][] keys(UUID edekId, String... labels) {
        if (closed) {
            throw new IllegalStateException("TokenKeyProvider is closed");
        }
        byte[][] out = new byte[labels.length][];
        byte[] dek = null;
        try {
            for (int i = 0; i < labels.length; i++) {
                String cacheKey = edekId + "/" + labels[i];
                byte[] cached = derived.get(cacheKey);
                if (cached == null) {
                    if (dek == null) {
                        dek = unwrap(edekId);
                    }
                    cached = hkdfSha256(dek, null, labels[i].getBytes(StandardCharsets.UTF_8), KEY_LENGTH);
                    derived.put(cacheKey, cached);
                }
                out[i] = cached;
            }
            return out;
        } finally {
            if (dek != null) {
                DekManager.zeroDek(dek);
            }
        }
    }

    private byte[] unwrap(UUID edekId) {
        try {
            byte[] dek = source.unwrap(edekId);
            if (dek == null || dek.length != DekManager.DEK_LENGTH_BYTES) {
                throw new IllegalStateException("unexpected DEK length");
            }
            return dek;
        } catch (RuntimeException e) {
            throw DeidException.badValue("could not unwrap key edek_id=" + edekId + ": " + e.getMessage(), e);
        }
    }

    /** Zeroes every derived key. Idempotent. */
    @Override
    public synchronized void close() {
        closed = true;
        derived.values().forEach(k -> Arrays.fill(k, (byte) 0));
        derived.clear();
    }

    /**
     * HKDF-SHA256 (RFC 5869). A null or empty {@code salt} means HashLen zero bytes,
     * as the RFC specifies.
     */
    public static byte[] hkdfSha256(byte[] ikm, byte[] salt, byte[] info, int length) {
        if (length <= 0 || length > 255 * HASH_LENGTH) {
            throw new IllegalArgumentException("HKDF output length out of range");
        }
        byte[] effectiveSalt = (salt == null || salt.length == 0) ? new byte[HASH_LENGTH] : salt;
        byte[] prk = hmacSha256(effectiveSalt, ikm);
        try {
            ByteArrayOutputStream okm = new ByteArrayOutputStream(length);
            byte[] t = new byte[0];
            for (int i = 1; okm.size() < length; i++) {
                byte[] next = hmacSha256(prk, t, info == null ? new byte[0] : info, new byte[]{(byte) i});
                Arrays.fill(t, (byte) 0);
                t = next;
                okm.write(t, 0, Math.min(t.length, length - okm.size()));
            }
            Arrays.fill(t, (byte) 0);
            return okm.toByteArray();
        } finally {
            Arrays.fill(prk, (byte) 0);
        }
    }

    /** HMAC-SHA256 over the concatenation of {@code parts}, in BC-FIPS. */
    public static byte[] hmacSha256(byte[] key, byte[]... parts) {
        try {
            Mac mac = Mac.getInstance(HMAC, BouncyCastleFipsProvider.PROVIDER_NAME);
            mac.init(new SecretKeySpec(key, HMAC));
            for (byte[] part : parts) {
                mac.update(part);
            }
            return mac.doFinal();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HMAC-SHA256 failed", e);
        }
    }
}
