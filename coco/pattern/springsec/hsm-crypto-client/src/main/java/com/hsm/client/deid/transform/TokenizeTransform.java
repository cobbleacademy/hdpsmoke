package com.hsm.client.deid.transform;

import com.hsm.client.deid.ColumnTransform;
import com.hsm.client.deid.FieldOptions;
import com.hsm.client.deid.FieldStats;
import com.hsm.client.deid.TokenKeyProvider;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@code tokenize}: a consistent code computed from the value (DEIDENTIFICATION.md
 * section 4.4).
 *
 * <pre>
 * token = prefix + base64url_nopad( HMAC-SHA256( K_tok, domain ‖ 0x00 ‖ normalize(value) )[0..16) )
 * member_id=M1234567  →  tk_Q2x9fP0aLmV3kR7tYw1bZg
 * </pre>
 *
 * <ul>
 *   <li>The same value, key and domain always give the same token, across runs.</li>
 *   <li>Not reversible. {@code K_tok} is derived from the profile's
 *       {@code token-key-edek-id}; one key per vendor makes tokens differ between
 *       vendors.</li>
 *   <li>A token is derived from the value, so a {@code safe-harbor} profile can't use
 *       it on an identifier (§164.514(c)); the validator enforces that.</li>
 * </ul>
 * Pinned by {@code deid-test-vectors.json}.
 */
public final class TokenizeTransform implements ColumnTransform {

    static final int TOKEN_BYTES = 16;
    static final String DEFAULT_PREFIX = "tk_";

    record Config(Normalize normalize, String prefix) {
    }

    private final Config config;
    private final byte[] key;
    private final byte[] domainPrefix;
    private final String domain;

    private TokenizeTransform(Config config, byte[] key, String domain) {
        this.config = config;
        this.key = key;
        this.domain = domain;
        this.domainPrefix = domainPrefix(domain);
    }

    /** @param key the derived tokenize key, owned by the {@link TokenKeyProvider}; not copied */
    public static TokenizeTransform create(FieldOptions options, byte[] key, String domain) {
        return new TokenizeTransform(parse(options), key, domain);
    }

    static Config parse(FieldOptions o) {
        Normalize normalize = Normalize.fromLabel(o.string("normalize", "trim"));
        String prefix = o.string("prefix", DEFAULT_PREFIX);
        o.rejectUnknown("tokenize");
        return new Config(normalize, prefix);
    }

    /** {@code domain ‖ 0x00}, the HMAC input prefix shared by tokenize and surrogate lookups. */
    static byte[] domainPrefix(String domain) {
        byte[] d = domain.getBytes(StandardCharsets.UTF_8);
        return Arrays.copyOf(d, d.length + 1);
    }

    /** The token for one already-normalized value. Package-visible for the test vectors. */
    static String token(byte[] key, String domain, String normalized, String prefix) {
        byte[] mac = TokenKeyProvider.hmacSha256(key, domainPrefix(domain), normalized.getBytes(StandardCharsets.UTF_8));
        return prefix + Base64.getUrlEncoder().withoutPadding().encodeToString(Arrays.copyOf(mac, TOKEN_BYTES));
    }

    @Override
    public Method method() {
        return Method.TOKENIZE;
    }

    @Override
    public String apply(String value, Map<String, String> record, FieldStats stats) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        String normalized = config.normalize().apply(value);
        byte[] mac = TokenKeyProvider.hmacSha256(key, domainPrefix, normalized.getBytes(StandardCharsets.UTF_8));
        return config.prefix() + Base64.getUrlEncoder().withoutPadding().encodeToString(Arrays.copyOf(mac, TOKEN_BYTES));
    }

    @Override
    public Map<String, Object> describe() {
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("domain", domain);
        d.put("normalize", config.normalize().label());
        return d;
    }
}
