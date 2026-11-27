package com.hsm.edekmigrate;

import javax.crypto.Cipher;
import javax.crypto.spec.OAEPParameterSpec;
import javax.crypto.spec.PSource;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.spec.MGF1ParameterSpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An HSM stand-in with real RSA-OAEP-256 keys per (KEK name, version), as Key Vault
 * does it: wrap uses a KEK's latest version, unwrap needs the exact version. Can be
 * told to fail chosen EDEKs, or everything, as a 403 or an outage would.
 */
final class FakeKekVault implements KekVault {

    private static final OAEPParameterSpec OAEP = new OAEPParameterSpec("SHA-256", "MGF1", MGF1ParameterSpec.SHA256,
            PSource.PSpecified.DEFAULT);

    private final Map<String, Map<String, KeyPair>> keys = new LinkedHashMap<>();
    final Set<String> failingEdeks = new HashSet<>();
    boolean failAll;
    int unwraps;
    int wraps;

    FakeKekVault kek(String name, String... versions) {
        for (String v : versions) {
            keys.computeIfAbsent(name, n -> new LinkedHashMap<>()).put(v, newKeyPair());
        }
        return this;
    }

    @Override
    public byte[] unwrap(byte[] edek, String kekName, String kekVersion) {
        unwraps++;
        if (failAll || failingEdeks.contains(Base64.getEncoder().encodeToString(edek))) {
            throw new IllegalStateException("injected failure");
        }
        KeyPair kp = key(kekName, kekVersion);
        try {
            Cipher c = Cipher.getInstance("RSA/ECB/OAEPPadding");
            c.init(Cipher.DECRYPT_MODE, kp.getPrivate(), OAEP);
            return c.doFinal(edek);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("unwrap failed", e);
        }
    }

    @Override
    public Wrapped wrap(byte[] dek, String kekName) {
        wraps++;
        if (failAll) {
            throw new IllegalStateException("injected failure");
        }
        Map<String, KeyPair> versions = keys.get(kekName);
        if (versions == null) {
            throw new IllegalStateException("no such key");
        }
        List<String> names = new ArrayList<>(versions.keySet());
        String latest = names.get(names.size() - 1);
        return new Wrapped(wrapWith(dek, kekName, latest), latest);
    }

    /** Wraps with a chosen version, for seeding A's rows on older versions. */
    byte[] wrapWith(byte[] dek, String kekName, String version) {
        try {
            Cipher c = Cipher.getInstance("RSA/ECB/OAEPPadding");
            c.init(Cipher.ENCRYPT_MODE, key(kekName, version).getPublic(), OAEP);
            return c.doFinal(dek);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    private KeyPair key(String kekName, String version) {
        Map<String, KeyPair> versions = keys.get(kekName);
        KeyPair kp = versions == null ? null : versions.get(version);
        if (kp == null) {
            throw new IllegalStateException("no such key version");
        }
        return kp;
    }

    private static KeyPair newKeyPair() {
        try {
            KeyPairGenerator g = KeyPairGenerator.getInstance("RSA");
            g.initialize(2048);
            return g.generateKeyPair();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }
}
