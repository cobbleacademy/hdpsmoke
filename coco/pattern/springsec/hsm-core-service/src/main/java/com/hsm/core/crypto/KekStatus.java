package com.hsm.core.crypto;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Locale;

/**
 * The attributes of a KEK's <b>latest</b> version that decide whether it can still
 * wrap: Key Vault refuses wrapKey/encrypt (403) on a version that is disabled, past
 * its expiry ({@code exp}) or before its not-before ({@code nbf}) -- but keeps
 * allowing unwrapKey/decrypt on an expired or not-yet-valid version, so existing
 * data stays readable. Only a disabled or deleted version breaks unwrap too.
 *
 * <p>The latest version is the one every new wrap uses (wraps go to the versionless
 * key id), so it is the one whose expiry matters. Older versions only unwrap, which
 * expiry doesn't block.
 *
 * @param notBefore null when the version has no not-before
 * @param expiresOn null when the version has no expiry
 */
public record KekStatus(String kekName, String version, boolean enabled, OffsetDateTime notBefore,
                        OffsetDateTime expiresOn) {

    public enum State {
        /** Wraps work, expiry beyond the warning window. */
        OK,
        /** Wraps work, no expiry set on the version (allowed, but not recommended). */
        NO_EXPIRY,
        /** Wraps work, but expiry is within the warning window. */
        EXPIRING,
        /** Past expiry: Key Vault refuses new wraps; unwraps still work. */
        EXPIRED,
        /** Before not-before: Key Vault refuses new wraps. */
        NOT_YET_VALID,
        /** Disabled: Key Vault refuses wraps and unwraps. */
        DISABLED;

        /** Lower-case name as it appears in logs and audit events. */
        public String label() {
            return name().toLowerCase(Locale.ROOT);
        }

        /** Can a new DEK still be wrapped under this version? */
        public boolean wrapsWork() {
            return this == OK || this == NO_EXPIRY || this == EXPIRING;
        }
    }

    /** A version with no restrictions -- what clients without expiry metadata (the demo HSM) report. */
    public static KekStatus withoutLimits(String kekName, String version) {
        return new KekStatus(kekName, version, true, null, null);
    }

    /** The version's state at {@code now}; EXPIRING when fewer than {@code warnDays} days are left. */
    public State state(Instant now, int warnDays) {
        if (!enabled) {
            return State.DISABLED;
        }
        if (notBefore != null && now.isBefore(notBefore.toInstant())) {
            return State.NOT_YET_VALID;
        }
        if (expiresOn == null) {
            return State.NO_EXPIRY;
        }
        if (!now.isBefore(expiresOn.toInstant())) {
            return State.EXPIRED;
        }
        return now.plus(Duration.ofDays(warnDays)).isBefore(expiresOn.toInstant()) ? State.OK : State.EXPIRING;
    }

    /** Can a new DEK be wrapped under this version at {@code now}? Independent of any warning window. */
    public boolean wrapsWork(Instant now) {
        return state(now, 0).wrapsWork();
    }

    /**
     * Whole days until expiry (rounded down), negative once expired; null without an expiry.
     * 0 means less than a day left.
     */
    public Long daysLeft(Instant now) {
        if (expiresOn == null) {
            return null;
        }
        // getSeconds() rounds down, so a version that expired a minute ago is -1, not 0.
        return Math.floorDiv(Duration.between(now, expiresOn.toInstant()).getSeconds(), 86_400L);
    }
}
