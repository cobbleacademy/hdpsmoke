package com.hsm.client.crypto;

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
 * Duplicated verbatim from com.hsm.bulk.crypto.DekManager (hsm-bulk-service), itself
 * duplicated from com.hsm.core.crypto.DekManager -- this repo has no shared library
 * module between Spring Boot modules (see cek-rotation-service's own FipsBootstrap
 * duplication for precedent). CLNT needs this for the single-record (BULK DB column)
 * and per-chunk (BULK File) local AES-GCM encrypt/decrypt, and for packToken/
 * unpackToken so the ciphertext_token it produces or consumes is byte-for-byte
 * compatible with hsm-core-service's own /encrypt and /decrypt.
 */
public final class DekManager {

    public static final int IV_LENGTH = 12;   // 96-bit nonce
    public static final int TAG_LENGTH = 16;  // 128-bit GCM authentication tag (bytes)
    private static final int TAG_LENGTH_BITS = TAG_LENGTH * 8;

    public static final int DEK_LENGTH_BYTES = 32; // 256-bit AES key
    public static final String ALGORITHM = "AES-256-GCM";

    private static final String CIPHER_TRANSFORMATION = "AES/GCM/NoPadding";
    private static final String PROVIDER = BouncyCastleFipsProvider.PROVIDER_NAME;

    private static final byte TOKEN_VERSION = 0x01;
    private static final String TOKEN_PREFIX = "v1.";
    private static final int TOKEN_FIXED_BYTES = 1 + 16 + IV_LENGTH + TAG_LENGTH; // 45 bytes

    private static final SecureRandom RANDOM = CryptoServicesRegistrar.getSecureRandom();

    private DekManager() {
    }

    public record EncryptResult(byte[] ciphertext, byte[] iv, byte[] tag) {
    }

    public record UnpackedToken(UUID edekId, byte[] iv, byte[] tag, byte[] ciphertext) {
    }

    public static byte[] generateDek() {
        byte[] raw = new byte[DEK_LENGTH_BYTES];
        RANDOM.nextBytes(raw);
        return raw;
    }

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

    public static void zeroDek(byte[] dek) {
        Arrays.fill(dek, (byte) 0);
    }

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
