package com.hsm.client.deid;

import com.hsm.client.deid.transform.DropTransform;
import com.hsm.client.deid.transform.GeneralizeTransform;
import com.hsm.client.deid.transform.MaskTransform;
import com.hsm.client.deid.transform.PassthroughTransform;
import com.hsm.client.deid.transform.SurrogateTransform;
import com.hsm.client.deid.transform.TokenizeTransform;

import java.time.Clock;
import java.util.Set;

/**
 * Builds each field's {@link ColumnTransform} from the profile and the run's
 * {@link TransformContext}: options parsed, keys derived, surrogate table attached.
 */
public final class TransformRegistry {

    private final DeidProfile profile;
    private final TransformContext context;

    public TransformRegistry(DeidProfile profile, TransformContext context) {
        this.profile = profile;
        this.context = context;
    }

    /**
     * Checks a field's options without keys, a store or a clock. Used by
     * {@link ProfileValidator}.
     *
     * @throws IllegalArgumentException naming the problem, without the field name
     */
    public static void checkOptions(DeidProfile profile, DeidProfile.Field field) {
        FieldOptions o = new FieldOptions(field.options());
        switch (field.method()) {
            case DROP -> DropTransform.checkOptions(o);
            case PASSTHROUGH -> PassthroughTransform.checkOptions(o);
            case MASK -> MaskTransform.create(o);
            case GENERALIZE -> GeneralizeTransform.create(o, Clock.systemUTC(), Set.of());
            case TOKENIZE -> TokenizeTransform.create(o, new byte[32], tokenDomain(profile, field));
            case SURROGATE -> SurrogateTransform.create(o, field.domain(), "check",
                    new byte[][]{new byte[32], new byte[32]}, java.util.UUID.randomUUID(), null, null);
        }
    }

    /** The generalize transform for {@code field}, or null; lets the validator see {@code to} and {@code as-of}. */
    static GeneralizeTransform generalize(DeidProfile.Field field) {
        if (field.method() != ColumnTransform.Method.GENERALIZE) {
            return null;
        }
        return GeneralizeTransform.create(new FieldOptions(field.options()), Clock.systemUTC(), Set.of());
    }

    static String tokenDomain(DeidProfile profile, DeidProfile.Field field) {
        String domain = field.domain() != null ? field.domain() : profile.tokenDomain();
        if (domain == null || domain.isBlank()) {
            throw new IllegalArgumentException("tokenize needs a domain (field domain or profile token-domain)");
        }
        return domain;
    }

    /**
     * The configured transform for one field.
     *
     * @throws IllegalArgumentException for bad options (run {@link ProfileValidator} first)
     * @throws IllegalStateException    when the run lacks keys or a surrogate table the field needs
     */
    public ColumnTransform create(DeidProfile.Field field) {
        FieldOptions o = new FieldOptions(field.options());
        return switch (field.method()) {
            case DROP -> {
                DropTransform.checkOptions(o);
                yield DropTransform.INSTANCE;
            }
            case PASSTHROUGH -> {
                PassthroughTransform.checkOptions(o);
                yield PassthroughTransform.INSTANCE;
            }
            case MASK -> MaskTransform.create(o);
            case GENERALIZE -> GeneralizeTransform.create(o, context.clock(), context.restrictedZip3());
            case TOKENIZE -> {
                if (profile.tokenKeyEdekId() == null) {
                    throw new IllegalStateException("tokenize needs token-key-edek-id in the profile");
                }
                byte[] key = keys().key(profile.tokenKeyEdekId(), TokenKeyProvider.TOKENIZE_LABEL);
                yield TokenizeTransform.create(o, key, tokenDomain(profile, field));
            }
            case SURROGATE -> {
                if (context.surrogateKeyEdekId() == null) {
                    throw new IllegalStateException("surrogate needs the surrogate-table key (client.deid.surrogate.key-edek-id)");
                }
                if (context.surrogateStore() == null) {
                    throw new IllegalStateException("surrogate needs the surrogate table (deid_surrogate)");
                }
                if (profile.surrogateScope() == null) {
                    throw new IllegalStateException("surrogate needs surrogate-scope in the profile");
                }
                byte[][] keys = keys().keys(context.surrogateKeyEdekId(), SurrogateTransform.keyLabels());
                yield SurrogateTransform.create(o, field.domain(), profile.surrogateScope(), keys,
                        context.surrogateKeyEdekId(), context.surrogateStore(), context.runId());
            }
        };
    }

    private TokenKeyProvider keys() {
        if (context.keys() == null) {
            throw new IllegalStateException("this profile needs keys from hsm-core, but the run has no TokenKeyProvider");
        }
        return context.keys();
    }
}
