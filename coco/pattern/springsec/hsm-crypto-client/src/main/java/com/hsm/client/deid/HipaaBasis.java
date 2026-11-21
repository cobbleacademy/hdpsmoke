package com.hsm.client.deid;

import java.util.Arrays;
import java.util.Locale;

/** The HIPAA basis a profile declares (DEIDENTIFICATION.md section 3). */
public enum HipaaBasis {
    /** Disclosure under a BAA; any method allowed, reviewed at profile approval. */
    MINIMUM_NECESSARY,
    /** De-identified under §164.514(b)(2); the validator enforces section 6.2. */
    SAFE_HARBOR,
    /** De-identified under an expert's report; the profile must cite it. */
    EXPERT_DETERMINATION;

    /** As written in a profile: {@code minimum-necessary}, {@code safe-harbor}, {@code expert-determination}. */
    public String label() {
        return name().toLowerCase(Locale.ROOT).replace('_', '-');
    }

    public static HipaaBasis fromLabel(String label) {
        return Arrays.stream(values())
                .filter(b -> b.label().equals(label))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("unknown hipaa-basis '" + label
                        + "' (expected minimum-necessary, safe-harbor or expert-determination)"));
    }
}
