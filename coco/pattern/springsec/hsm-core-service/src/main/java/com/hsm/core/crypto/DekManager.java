package com.hsm.core.crypto;

import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;

import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.UUID;

/**
 * DEK lifecycle: generate, encrypt, decrypt using AES-256-GCM (FIPS 140-3 approved),
 * backed by the CMVP-validated Bouncy Castle FIPS Java API provider (cert #4943).
 *
 * <p>AAD (Additional Authenticated Data) binds ciphertext to the originating app_id.
 * This prevents a stolen ciphertext blob from being decrypted by a different app,
 * even if that app obtains the EDEK.
 *
 * <p>Token layout (binary, inside base64url wrapper):
 * <pre>
 *   1 byte  : format version (currently 0x01)
 *  16 bytes : edek_id UUID (big-endian)
 *  12 bytes : AES-GCM IV (nonce)
 *  16 bytes : AES-GCM authentication tag
 *   N bytes : ciphertext (variable)
 * </pre>
 * On-wire: {@code "v1.<base64url(binary)>"}. The "v1." prefix lets future
 * parsers detect the version before decoding.
 */
public final class DekManager {

    public static final int IV_LENGTH = 12;   // 96-bit nonce
    public static final int TAG_LENGTH = 16;  // 128-bit GCM authentication tag (bytes)
    private static final int TAG_LENGTH_BITS = TAG_LENGTH * 8;

    public static final int DEK_LENGTH_BYTES = 32; // 256-bit AES key
    public static final String ALGORITHM = "AES-256-GCM"; // persisted per-record so future algorithm migrations stay decryptable

    private static final String CIPHER_TRANSFORMATION = "AES/GCM/NoPadding";
    private static final String PROVIDER = BouncyCastleFipsProvider.PROVIDER_NAME;

    private static final byte TOKEN_VERSION = 0x01;
    private static final String TOKEN_PREFIX = "v1.";
    private static final int TOKEN_FIXED_BYTES = 1 + 16 + IV_LENGTH + TAG_LENGTH; // 45 bytes

    // FIPS-approved-mode DRBG-backed SecureRandom, rather than a plain java.security.SecureRandom.
    private static final SecureRandom RANDOM = CryptoServicesRegistrar.getSecureRandom();

    private DekManager() {
    }

    public record EncryptResult(byte[] ciphertext, byte[] iv, byte[] tag) {
    }

    public record UnpackedToken(UUID edekId, byte[] iv, byte[] tag, byte[] ciphertext) {
    }

    /** Return a fresh 256-bit DEK. Caller is responsible for zeroing it after use via {@link #zeroDek}. */
    public static byte[] generateDek() {
        byte[] raw = new byte[DEK_LENGTH_BYTES];
        RANDOM.nextBytes(raw);
        return raw;
    }

    /**
     * AES-256-GCM encrypt. The JCE cipher appends the 16-byte GCM tag to the
     * ciphertext; we split them for explicit storage, mirroring the Python
     * {@code cryptography} library's output shape.
     */
    public static EncryptResult encrypt(byte[] plaintext, byte[] dek, String appId) {
        byte[] iv = IvFactory.generate();
        byte[] aad = makeAad(appId);
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_TRANSFORMATION, PROVIDER);
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(dek, "AES"), new GCMParameterSpec(TAG_LENGTH_BITS, iv));
            cipher.updateAAD(aad);
            byte[] combined = cipher.doFinal(plaintext);
            int ctLen = combined.length - TAG_LENGTH;
            byte[] ciphertext = Arrays.copyOfRange(combined, 0, ctLen);
            byte[] tag = Arrays.copyOfRange(combined, ctLen, combined.length);
            return new EncryptResult(ciphertext, iv, tag);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("AES-GCM encryption failed", e);
        }
    }

    /**
     * AES-256-GCM decrypt + tag verification. Throws {@link AEADBadTagException}
     * on authentication failure (tampered ciphertext or wrong app_id) — mirrors
     * Python's {@code cryptography.exceptions.InvalidTag}.
     */
    public static byte[] decrypt(byte[] ciphertext, byte[] tag, byte[] iv, byte[] dek, String appId) throws AEADBadTagException {
        byte[] aad = makeAad(appId);
        byte[] combined = new byte[ciphertext.length + tag.length];
        System.arraycopy(ciphertext, 0, combined, 0, ciphertext.length);
        System.arraycopy(tag, 0, combined, ciphertext.length, tag.length);
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_TRANSFORMATION, PROVIDER);
            cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(dek, "AES"), new GCMParameterSpec(TAG_LENGTH_BITS, iv));
            cipher.updateAAD(aad);
            return cipher.doFinal(combined);
        } catch (AEADBadTagException e) {
            throw e;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("AES-GCM decryption failed", e);
        }
    }

    /** Overwrite DEK bytes in memory immediately after use. */
    public static void zeroDek(byte[] dek) {
        Arrays.fill(dek, (byte) 0);
    }

    /**
     * Encode all decrypt inputs into one opaque token the client stores and echoes back.
     * Binary layout: version(1) | edek_id(16) | iv(12) | tag(16) | ciphertext(N).
     * On-wire: "v1.&lt;base64url(binary)&gt;"
     */
    public static String packToken(UUID edekId, byte[] iv, byte[] tag, byte[] ciphertext) {
        ByteBuffer buf = ByteBuffer.allocate(TOKEN_FIXED_BYTES + ciphertext.length);
        buf.put(TOKEN_VERSION);
        buf.putLong(edekId.getMostSignificantBits());
        buf.putLong(edekId.getLeastSignificantBits());
        buf.put(iv);
        buf.put(tag);
        buf.put(ciphertext);
        return TOKEN_PREFIX + Base64.getUrlEncoder().encodeToString(buf.array());
    }

    /**
     * Decode a ciphertext_token produced by {@link #packToken}.
     * Throws {@link IllegalArgumentException} with a descriptive message on any format error.
     */
    public static UnpackedToken unpackToken(String token) {
        if (!token.startsWith(TOKEN_PREFIX)) {
            throw new IllegalArgumentException(
                    "ciphertext_token has unrecognised format: expected prefix '" + TOKEN_PREFIX + "'");
        }
        String b64Part = token.substring(TOKEN_PREFIX.length());
        byte[] binary;
        try {
            binary = Base64.getUrlDecoder().decode(padBase64(b64Part));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("ciphertext_token contains invalid base64url data");
        }

        if (binary.length < TOKEN_FIXED_BYTES) {
            throw new IllegalArgumentException(
                    "ciphertext_token is too short: " + binary.length + " bytes (minimum " + TOKEN_FIXED_BYTES + ")");
        }

        byte version = binary[0];
        if (version != TOKEN_VERSION) {
            throw new IllegalArgumentException(String.format(
                    "ciphertext_token uses unsupported version 0x%02x; this service supports 0x%02x",
                    version, TOKEN_VERSION));
        }

        ByteBuffer buf = ByteBuffer.wrap(binary);
        buf.get(); // version, already validated
        long msb = buf.getLong();
        long lsb = buf.getLong();
        UUID edekId = new UUID(msb, lsb);
        byte[] iv = new byte[IV_LENGTH];
        buf.get(iv);
        byte[] tag = new byte[TAG_LENGTH];
        buf.get(tag);
        byte[] ciphertext = new byte[buf.remaining()];
        buf.get(ciphertext);

        if (ciphertext.length == 0) {
            throw new IllegalArgumentException("ciphertext_token contains no ciphertext payload");
        }

        return new UnpackedToken(edekId, iv, tag, ciphertext);
    }

    /**
     * First 8 bytes of SHA-256(iv || tag) as a 16-char hex string.
     * Stored with the EDEK record so decrypt can detect element mix-ups
     * before AES-GCM decryption even runs. Not a secret — it's a consistency
     * check, not a MAC; the actual authentication is AES-GCM's tag.
     */
    public static String makeFingerprint(byte[] iv, byte[] tag) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(iv);
            digest.update(tag);
            byte[] hash = digest.digest();
            return HexFormat.of().formatHex(hash, 0, 8);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] makeAad(String appId) {
        return ("hsm-svc:app_id=" + appId).getBytes(StandardCharsets.UTF_8);
    }

    private static String padBase64(String s) {
        int mod = s.length() % 4;
        return mod == 0 ? s : s + "=".repeat(4 - mod);
    }
}
