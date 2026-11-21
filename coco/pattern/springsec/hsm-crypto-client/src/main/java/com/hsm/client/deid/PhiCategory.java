package com.hsm.client.deid;

import java.util.Arrays;
import java.util.Locale;

/**
 * The {@code phi:} tag on a profile field: which Safe Harbor identifier it holds. The
 * group decides which methods a {@code safe-harbor} profile may use on it
 * (DEIDENTIFICATION.md section 6.2).
 */
public enum PhiCategory {
    NAME(Group.DIRECT),
    ADDRESS(Group.DIRECT),
    EMAIL(Group.DIRECT),
    PHONE(Group.DIRECT),
    FAX(Group.DIRECT),
    SSN(Group.DIRECT),
    URL(Group.DIRECT),
    IP(Group.DIRECT),
    DEVICE_ID(Group.DIRECT),
    LICENSE(Group.DIRECT),
    VEHICLE_ID(Group.DIRECT),
    BIOMETRIC(Group.DIRECT),
    PHOTO(Group.DIRECT),
    FREE_TEXT(Group.DIRECT),

    MEMBER_ID(Group.UNIQUE_ID),
    MRN(Group.UNIQUE_ID),
    ACCOUNT(Group.UNIQUE_ID),
    CARD_NUMBER(Group.UNIQUE_ID),
    PLAN_ID(Group.UNIQUE_ID),
    UNIQUE_ID(Group.UNIQUE_ID),

    BIRTH_DATE(Group.BIRTH_DATE),
    DATE(Group.DATE),
    ZIP(Group.ZIP),
    AGE(Group.AGE);

    /** How Safe Harbor treats the category. */
    public enum Group {
        /** Direct identifiers: drop only. */
        DIRECT,
        /** Unique IDs: drop, or a surrogate as a join key. */
        UNIQUE_ID,
        /** Birth dates: drop, or the year capped for ages over 89. */
        BIRTH_DATE,
        /** Other dates (issue, redemption, service): drop, or the year. */
        DATE,
        /** ZIP codes: drop, or ZIP3 with restricted areas as 000. */
        ZIP,
        /** Ages: drop, or capped at 90+. */
        AGE
    }

    private final Group group;

    PhiCategory(Group group) {
        this.group = group;
    }

    public Group group() {
        return group;
    }

    /** As written in a profile, e.g. {@code card-number}. */
    public String label() {
        return name().toLowerCase(Locale.ROOT).replace('_', '-');
    }

    public static PhiCategory fromLabel(String label) {
        return Arrays.stream(values())
                .filter(c -> c.label().equals(label))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("unknown phi category '" + label + "'"));
    }
}
