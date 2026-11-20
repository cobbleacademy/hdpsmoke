package com.hsm.core.service;

import com.hsm.core.config.HsmProperties;
import com.hsm.core.crypto.KekClient;
import com.hsm.core.crypto.KekStatus;
import com.hsm.core.model.KekRegistryEntry;
import com.hsm.core.model.RotationStatus;
import com.hsm.core.repository.EdekRecordRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;

/**
 * Watches the expiry of every KEK in use, so a KEK whose latest version expires (or
 * is disabled) is seen before it stops new encrypts -- not after.
 *
 * <p>Why it matters: every new wrap goes to the KEK's latest version, and Key Vault
 * refuses wraps on a version past its expiry. Decrypts keep working, and the old
 * reachability-only health check kept saying "ok", so the first sign was failing
 * encrypts and a failing rotation job.
 *
 * <p>"In use" is the union of the legacy default KEK, every kek_name in kek_registry
 * (where new data goes) and every kek_name on a current edek_records row (what
 * rotation re-wraps into). One Key Vault GET per KEK; only the latest version is
 * checked, because older versions only unwrap, which expiry doesn't block.
 *
 * <p>{@link #refresh()} always asks Key Vault (the daily check);
 * {@link #snapshot()} reuses the last result for hsm.kek-expiry.cache-ttl-seconds
 * (/admin/health, which probes call every few seconds). Neither throws: a KEK whose
 * lookup fails is reported with its error.
 */
@Service
public class KekExpiryMonitor {

    private static final Logger log = LoggerFactory.getLogger(KekExpiryMonitor.class);

    private final KekClient kekClient;
    private final KekRegistryService kekRegistryService;
    private final EdekRecordRepository edekRecordRepository;
    private final int warnDays;
    private final Duration cacheTtl;
    private final Clock clock;

    private volatile Snapshot cached;

    @Autowired
    public KekExpiryMonitor(KekClient kekClient, KekRegistryService kekRegistryService,
                            EdekRecordRepository edekRecordRepository, HsmProperties properties) {
        this(kekClient, kekRegistryService, edekRecordRepository, properties, Clock.systemUTC());
    }

    KekExpiryMonitor(KekClient kekClient, KekRegistryService kekRegistryService,
                     EdekRecordRepository edekRecordRepository, HsmProperties properties, Clock clock) {
        this.kekClient = kekClient;
        this.kekRegistryService = kekRegistryService;
        this.edekRecordRepository = edekRecordRepository;
        this.warnDays = properties.kekExpiry().warnDays();
        this.cacheTtl = Duration.ofSeconds(Math.max(0, properties.kekExpiry().cacheTtlSeconds()));
        this.clock = clock;
    }

    public int warnDays() {
        return warnDays;
    }

    /** One KEK's result: its status, or why it couldn't be read. Exactly one of the two is set. */
    public record Check(String kekName, KekStatus status, String error) {

        /** Lower-case state label, "unknown" when the lookup failed. */
        public String stateLabel(Instant now, int warnDays) {
            return status == null ? "unknown" : status.state(now, warnDays).label();
        }

        /**
         * Is this KEK a problem right now? Expired, disabled or not yet valid -- or
         * unreadable, which with the vault itself reachable usually means the key was
         * deleted or the app lost access to it.
         */
        public boolean failing(Instant now) {
            return status == null || !status.wrapsWork(now);
        }
    }

    public record Snapshot(Instant takenAt, List<Check> checks) {

        /** True when no KEK is failing (see {@link Check#failing}). */
        public boolean allUsable(Instant now) {
            return checks.stream().noneMatch(c -> c.failing(now));
        }

        /** The checked KEK version that expires first, if any has an expiry. */
        public Optional<KekStatus> soonestExpiry() {
            return checks.stream()
                    .map(Check::status)
                    .filter(Objects::nonNull)
                    .filter(s -> s.expiresOn() != null)
                    .min(Comparator.comparing(KekStatus::expiresOn));
        }
    }

    public Instant now() {
        return clock.instant();
    }

    /** The last result while younger than the cache TTL, otherwise a fresh {@link #refresh()}. */
    public Snapshot snapshot() {
        Snapshot current = cached;
        if (current != null && current.takenAt().plus(cacheTtl).isAfter(clock.instant())) {
            return current;
        }
        return refresh();
    }

    /** Looks up every KEK in use in Key Vault now, and caches the result. */
    public synchronized Snapshot refresh() {
        List<Check> checks = new ArrayList<>();
        for (String kekName : kekNamesInUse()) {
            try {
                checks.add(new Check(kekName, kekClient.getKekStatus(kekName), null));
            } catch (RuntimeException e) {
                checks.add(new Check(kekName, null, e.getClass().getSimpleName() + ": " + e.getMessage()));
            }
        }
        Snapshot snapshot = new Snapshot(clock.instant(), List.copyOf(checks));
        cached = snapshot;
        return snapshot;
    }

    /**
     * Sorted, de-duplicated. If the database can't be read, falls back to the legacy
     * default KEK alone rather than failing -- the database problem has its own signal.
     */
    List<String> kekNamesInUse() {
        TreeSet<String> names = new TreeSet<>();
        String legacyDefault = kekRegistryService.getLegacyDefaultKekName();
        if (legacyDefault != null && !legacyDefault.isBlank()) {
            names.add(legacyDefault);
        }
        try {
            for (KekRegistryEntry entry : kekRegistryService.listEntries()) {
                if (entry.getKekName() != null && !entry.getKekName().isBlank()) {
                    names.add(entry.getKekName());
                }
            }
            for (String kekName : edekRecordRepository.findDistinctKekNamesForCurrentRecords(RotationStatus.CURRENT)) {
                if (kekName != null && !kekName.isBlank()) {
                    names.add(kekName);
                }
            }
        } catch (RuntimeException e) {
            log.warn("kek_expiry_kek_names_lookup_failed error={}", e.getMessage());
        }
        return new ArrayList<>(names);
    }
}
