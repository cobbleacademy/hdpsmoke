package com.hsm.client.crypto;

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
 * Duplicated from com.hsm.bulk.crypto.TransportWrapper. CLNT is the actual intended
 * caller of {@link #unwrap} -- SVC (hsm-bulk-service) only ever wraps, since the
 * private key never leaves CLNT by design. {@link #wrap} is kept here too (unused by
 * DbBulkJob/FileBulkJob, which only ever unwrap what SVC sends) purely for symmetry
 * and any future test/tooling need to construct a wrapped DEK locally.
 */
public final class TransportWrapper {

    private static final String CIPHER_TRANSFORMATION = "RSA/ECB/OAEPWithSHA-256AndMGF1Padding";
    private static final String PROVIDER = BouncyCastleFipsProvider.PROVIDER_NAME;

    private TransportWrapper() {
    }

    public static byte[] wrap(byte[] dek, PublicKey publicKey) {
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_TRANSFORMATION, PROVIDER);
            cipher.init(Cipher.WRAP_MODE, publicKey);
            return cipher.wrap(new SecretKeySpec(dek, "AES"));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("RSA-OAEP-256 transport wrap failed", e);
        }
    }

    /**
     * BC-FIPS, running in approved-only mode, only permits RSA transformations via
     * Cipher.WRAP_MODE/UNWRAP_MODE for key-wrapping use -- see SVC's TransportWrapper
     * for the full explanation. This is CLNT's actual hot path: every /dek/issue and
     * /dek/unwrap response's wrapped_dek_b64 goes through this.
     */
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

    public static PublicKey parsePublicKeyPem(String pem) {
        try {
            byte[] decoded = Base64.getDecoder().decode(stripPemHeaders(pem, "PUBLIC KEY"));
            KeyFactory kf = KeyFactory.getInstance("RSA");
            return kf.generatePublic(new X509EncodedKeySpec(decoded));
        } catch (NoSuchAlgorithmException | InvalidKeySpecException | IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid public key PEM: " + e.getMessage(), e);
        }
    }

    /** CLNT's own private key, corresponding to the public key registered on app_registrations.public_key_pem. */
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
