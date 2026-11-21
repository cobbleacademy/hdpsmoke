package com.hsm.client.deid.transform;

import com.hsm.client.deid.ColumnTransform;
import com.hsm.client.deid.DeidException;
import com.hsm.client.deid.FieldOptions;
import com.hsm.client.deid.FieldStats;
import com.hsm.client.deid.SurrogateStore;
import com.hsm.client.deid.TokenKeyProvider;
import com.hsm.crypto.FipsBootstrap;
import com.hsm.crypto.IvFactory;
import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;

import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * {@code surrogate}: a random ID kept in the surrogate table (DEIDENTIFICATION.md
 * sections 4.5 and 8).
 *
 * <pre>
 * member_id=M1234567  →  S-8F2KQ-91QDM
 * deid_surrogate: (scope, domain, lookup_hash) ↔ S-8F2KQ-91QDM, value_enc
 * </pre>
 *
 * <ul>
 *   <li>The first time a value is seen in a scope, a random surrogate is created and
 *       stored; later runs reuse it.</li>
 *   <li>{@code lookup_hash = HMAC(K_lookup, domain ‖ 0x00 ‖ normalize(value))}: rows are
 *       found by equality without storing the value in clear.</li>
 *   <li>{@code value_enc = IV ‖ AES-256-GCM(K_enc, normalize(value)) ‖ tag}, with AAD
 *       binding it to scope and domain, so a surrogate can be mapped back on request
 *       ({@link #decryptValue}).</li>
 *   <li>Format: {@code prefix} ({@code S-}) + {@code length} (10) Crockford base32
 *       characters from the FIPS DRBG, in groups of 5. 10 characters are 50 bits.</li>
 * </ul>
 *
 * <p>Works a page at a time (section 8.3): look up the page's distinct values, insert
 * the missing ones, re-read the rows another run inserted first, and retry a surrogate
 * that collides up to {@value #MAX_COLLISION_RETRIES} times. Two runs for the same scope
 * therefore always agree on one surrogate.
 */
public final class SurrogateTransform implements ColumnTransform {

    static final int MAX_COLLISION_RETRIES = 5;
    static final String DEFAULT_PREFIX = "S-";
    static final int DEFAULT_LENGTH = 10;
    private static final char[] CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ".toCharArray();
    private static final int GROUP = 5;
    private static final int TAG_BITS = 128;
    private static final HexFormat HEX = HexFormat.of();

    static {
        FipsBootstrap.register();
    }

    private static final SecureRandom RANDOM = CryptoServicesRegistrar.getSecureRandom();

    record Config(String domain, Normalize normalize, String prefix, int length) {
    }

    private final Config config;
    private final String scope;
    private final byte[] lookupKey;
    private final byte[] encKey;
    private final UUID keyEdekId;
    private final SurrogateStore store;
    private final String runId;
    private final byte[] domainPrefix;

    private SurrogateTransform(Config config, String scope, byte[] lookupKey, byte[] encKey, UUID keyEdekId,
                               SurrogateStore store, String runId) {
        this.config = config;
        this.scope = scope;
        this.lookupKey = lookupKey;
        this.encKey = encKey;
        this.keyEdekId = keyEdekId;
        this.store = store;
        this.runId = runId;
        this.domainPrefix = TokenizeTransform.domainPrefix(config.domain());
    }

    /**
     * @param keys the lookup and encryption keys derived from {@code keyEdekId}, owned by
     *             the {@link TokenKeyProvider}; not copied
     */
    public static SurrogateTransform create(FieldOptions options, String domain, String scope, byte[][] keys,
                                            UUID keyEdekId, SurrogateStore store, String runId) {
        return new SurrogateTransform(parse(options, domain), scope, keys[0], keys[1], keyEdekId, store, runId);
    }

    /** The labels {@link #create} expects keys for, in order. */
    public static String[] keyLabels() {
        return new String[]{TokenKeyProvider.SURROGATE_LOOKUP_LABEL, TokenKeyProvider.SURROGATE_ENC_LABEL};
    }

    static Config parse(FieldOptions o, String domain) {
        Normalize normalize = Normalize.fromLabel(o.string("normalize", "trim"));
        String prefix = o.string("prefix", DEFAULT_PREFIX);
        int length = o.integer("length", DEFAULT_LENGTH, 8, 26);
        o.rejectUnknown("surrogate");
        if (domain == null || domain.isBlank()) {
            throw new IllegalArgumentException("surrogate needs a domain (e.g. member, card)");
        }
        return new Config(domain, normalize, prefix, length);
    }

    @Override
    public Method method() {
        return Method.SURROGATE;
    }

    @Override
    public String apply(String value, Map<String, String> record, FieldStats stats) {
        return applyPage(java.util.Collections.singletonList(value), java.util.Collections.singletonList(record), stats)
                .get(0);
    }

    @Override
    public List<String> applyPage(List<String> values, List<Map<String, String>> records, FieldStats stats) {
        // Distinct normalized values in this page, keyed by lookup hash (hex).
        Map<String, String> normalizedByHash = new LinkedHashMap<>();
        Map<String, byte[]> hashByHex = new LinkedHashMap<>();
        String[] hexByIndex = new String[values.size()];
        for (int i = 0; i < values.size(); i++) {
            String v = values.get(i);
            if (v == null || v.isEmpty()) {
                continue;
            }
            String normalized;
            try {
                normalized = config.normalize().apply(v);
            } catch (DeidException e) {
                throw e.atPageIndex(i);
            }
            byte[] hash = lookupHash(normalized);
            String hex = HEX.formatHex(hash);
            hexByIndex[i] = hex;
            normalizedByHash.putIfAbsent(hex, normalized);
            hashByHex.putIfAbsent(hex, hash);
        }

        Map<String, String> surrogates = new LinkedHashMap<>();
        if (!hashByHex.isEmpty()) {
            surrogates.putAll(store.find(scope, config.domain(), new ArrayList<>(hashByHex.values())));
            stats.add("existing", surrogates.size());

            List<byte[]> lostRaces = new ArrayList<>();
            for (Map.Entry<String, byte[]> e : hashByHex.entrySet()) {
                if (surrogates.containsKey(e.getKey())) {
                    continue;
                }
                String inserted = insertNew(e.getValue(), normalizedByHash.get(e.getKey()));
                if (inserted != null) {
                    surrogates.put(e.getKey(), inserted);
                    stats.increment("new");
                } else {
                    lostRaces.add(e.getValue());
                }
            }
            if (!lostRaces.isEmpty()) {
                Map<String, String> reread = store.find(scope, config.domain(), lostRaces);
                if (reread.size() != lostRaces.size()) {
                    throw DeidException.badValue("surrogate row reported as existing but not found on re-read");
                }
                surrogates.putAll(reread);
                stats.add("existing", reread.size());
            }
        }

        List<String> out = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            out.add(hexByIndex[i] == null ? values.get(i) : surrogates.get(hexByIndex[i]));
        }
        return out;
    }

    /** Inserts a new row; returns its surrogate, or null if another run inserted the row first. */
    private String insertNew(byte[] hash, String normalized) {
        byte[] valueEnc = encryptValue(normalized);
        for (int attempt = 0; attempt < MAX_COLLISION_RETRIES; attempt++) {
            String surrogate = newSurrogate();
            switch (store.insertIfAbsent(scope, config.domain(), hash, valueEnc, keyEdekId, surrogate, runId)) {
                case INSERTED -> {
                    return surrogate;
                }
                case ROW_EXISTS -> {
                    return null;
                }
                case SURROGATE_TAKEN -> {
                    // pick another
                }
            }
        }
        throw DeidException.badValue("could not find an unused surrogate after " + MAX_COLLISION_RETRIES
                + " tries; increase the surrogate length");
    }

    byte[] lookupHash(String normalized) {
        return TokenKeyProvider.hmacSha256(lookupKey, domainPrefix, normalized.getBytes(StandardCharsets.UTF_8));
    }

    String newSurrogate() {
        StringBuilder sb = new StringBuilder(config.prefix());
        for (int i = 0; i < config.length(); i++) {
            if (i > 0 && i % GROUP == 0) {
                sb.append('-');
            }
            sb.append(CROCKFORD[RANDOM.nextInt(CROCKFORD.length)]);
        }
        return sb.toString();
    }

    private byte[] aad() {
        return ("hsm-deid-surrogate-v1|" + scope + "|" + config.domain()).getBytes(StandardCharsets.UTF_8);
    }

    byte[] encryptValue(String normalized) {
        byte[] iv = IvFactory.generate();
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding", BouncyCastleFipsProvider.PROVIDER_NAME);
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(encKey, "AES"), new GCMParameterSpec(TAG_BITS, iv));
            cipher.updateAAD(aad());
            byte[] ctAndTag = cipher.doFinal(normalized.getBytes(StandardCharsets.UTF_8));
            byte[] out = Arrays.copyOf(iv, iv.length + ctAndTag.length);
            System.arraycopy(ctAndTag, 0, out, iv.length, ctAndTag.length);
            return out;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("AES-GCM encryption failed", e);
        }
    }

    /**
     * Maps a stored {@code value_enc} back to the normalized value. For answering a
     * vendor's question about a surrogate; never used during an export.
     */
    public String decryptValue(byte[] valueEnc) {
        if (valueEnc.length < IvFactory.IV_LENGTH_BYTES + TAG_BITS / 8) {
            throw DeidException.badValue("value_enc too short");
        }
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding", BouncyCastleFipsProvider.PROVIDER_NAME);
            cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(encKey, "AES"),
                    new GCMParameterSpec(TAG_BITS, valueEnc, 0, IvFactory.IV_LENGTH_BYTES));
            cipher.updateAAD(aad());
            byte[] plain = cipher.doFinal(valueEnc, IvFactory.IV_LENGTH_BYTES, valueEnc.length - IvFactory.IV_LENGTH_BYTES);
            return new String(plain, StandardCharsets.UTF_8);
        } catch (AEADBadTagException e) {
            throw DeidException.badValue("value_enc failed authentication (wrong key, scope or domain)", e);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("AES-GCM decryption failed", e);
        }
    }

    @Override
    public Map<String, Object> describe() {
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("domain", config.domain());
        d.put("scope", scope);
        d.put("normalize", config.normalize().label());
        d.put("key_edek_id", keyEdekId.toString());
        return d;
    }
}
