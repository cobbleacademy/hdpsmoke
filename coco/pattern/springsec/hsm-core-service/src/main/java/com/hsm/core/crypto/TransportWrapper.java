package com.hsm.core.crypto;

import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;

import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

/**
 * RSA-OAEP-256 wrap/unwrap of a raw DEK for transport to/from the calling app (CLNT),
 * per java/docs/BULK_OPERATIONS.md's Tier 3 design -- same algorithm family already
 * used for KEK wrapping in AzureKeyVaultKekClient (KeyWrapAlgorithm.RSA_OAEP_256), so
 * no new crypto primitive is introduced here.
 *
 * <p>hsm-core-service itself only ever calls {@link #wrap}: the app's private key
 * never leaves CLNT by design, so this can encrypt DEK bytes for a specific app's
 * public key but can never decrypt one back. {@link #unwrap} exists so the
 * CLNT-side reference client (BulkVsBatchBenchmark) and round-trip tests can play
 * CLNT's role locally.
 */
public final class TransportWrapper {

    private static final String CIPHER_TRANSFORMATION = "RSA/ECB/OAEPWithSHA-256AndMGF1Padding";
    private static final String PROVIDER = BouncyCastleFipsProvider.PROVIDER_NAME;

    private TransportWrapper() {
    }

    /**
     * BC-FIPS, running in approved-only mode (FipsBootstrap), only permits RSA
     * transformations via Cipher.WRAP_MODE/UNWRAP_MODE for key-wrapping use --
     * plain ENCRYPT_MODE/DECRYPT_MODE with doFinal(byte[]) throws
     * "Cipher available for WRAP_MODE and UNWRAP_MODE only". This is semantically
     * correct anyway: a DEK is a key being wrapped, not arbitrary data being
     * encrypted -- the same operation Azure Key Vault's wrapKey/unwrapKey API
     * performs for the real KEK.
     */
    public static byte[] wrap(byte[] dek, PublicKey publicKey) {
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_TRANSFORMATION, PROVIDER);
            cipher.init(Cipher.WRAP_MODE, publicKey);
            return cipher.wrap(new SecretKeySpec(dek, "AES"));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("RSA-OAEP-256 transport wrap failed", e);
        }
    }

    public static byte[] unwrap(byte[] wrappedDek, PrivateKey privateKey) {
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_TRANSFORMATION, PROVIDER);
            cipher.init(Cipher.UNWRAP_MODE, privateKey);
            Key unwrapped = cipher.unwrap(wrappedDek, "AES", Cipher.SECRET_KEY);
            return unwrapped.getEncoded();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("RSA-OAEP-256 transport unwrap failed", e);
        }
    }

    /** Parse a PEM-encoded ("-----BEGIN PUBLIC KEY-----...") RSA public key, as stored in app_registrations.public_key_pem. */
    public static PublicKey parsePublicKeyPem(String pem) {
        try {
            byte[] decoded = Base64.getDecoder().decode(stripPemHeaders(pem, "PUBLIC KEY"));
            KeyFactory kf = KeyFactory.getInstance("RSA");
            return kf.generatePublic(new X509EncodedKeySpec(decoded));
        } catch (NoSuchAlgorithmException | InvalidKeySpecException | IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid public_key_pem: " + e.getMessage(), e);
        }
    }

    /** Parse a PEM-encoded PKCS#8 RSA private key -- CLNT-side use only (the benchmark's reference client). */
    public static PrivateKey parsePrivateKeyPem(String pem) {
        try {
            byte[] decoded = Base64.getDecoder().decode(stripPemHeaders(pem, "PRIVATE KEY"));
            KeyFactory kf = KeyFactory.getInstance("RSA");
            return kf.generatePrivate(new PKCS8EncodedKeySpec(decoded));
        } catch (NoSuchAlgorithmException | InvalidKeySpecException | IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid private key PEM: " + e.getMessage(), e);
        }
    }

    private static String stripPemHeaders(String pem, String label) {
        return pem
                .replace("-----BEGIN " + label + "-----", "")
                .replace("-----END " + label + "-----", "")
                .replaceAll("\\s", "");
    }
}
