package com.hsm.edekmigrate;

import com.azure.core.exception.HttpResponseException;

import java.io.PrintStream;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

/**
 * The tool's commands (java/docs/EDEK_TENANT_MIGRATION.md section 5). Each returns
 * the process exit status: {@link #OK}, {@link #STOPPED} (an error ended the run),
 * {@link #USAGE}, or {@link #PROBLEMS} (the run finished, but found problems or
 * refused).
 *
 * <p>The DEK exists only inside {@link #wrap} and {@link #verify}, for one row at a
 * time, and is zeroed in a finally block. Nothing about it is logged, stored or put in
 * an error: a failed row records only the error's type, and the terminal gets the
 * reason the vault or database gave ({@link Errors}).
 */
public final class Commands {

    public static final int OK = 0;
    public static final int STOPPED = 1;
    public static final int USAGE = 2;
    public static final int PROBLEMS = 3;

    private static final int FAILED_LIST_LIMIT = 200;
    private static final int LISTED_LIMIT = 10_000;

    private final MigrateConfig config;
    private final Connection sourceDb;
    private final Connection targetDb;
    private final KekVault sourceVault;
    private final KekVault targetVault;
    private final PrintStream out;
    private final SourceStore source;
    private final StagingStore staging;
    private final String sourceTable;
    private final String targetTable;
    private final String stagingTable;
    private final String appsTable;

    // What the command is doing now, for the error line if a database call fails.
    private String stepSide = "source";
    private String step = "starting";

    public Commands(MigrateConfig config, Connection sourceDb, Connection targetDb, KekVault sourceVault,
                    KekVault targetVault, PrintStream out) throws SQLException {
        this.config = config;
        this.sourceDb = sourceDb;
        this.targetDb = targetDb;
        this.sourceVault = sourceVault;
        this.targetVault = targetVault;
        this.out = out;
        this.sourceTable = config.source().cryptoSchema() + ".edek_records";
        this.targetTable = config.target().cryptoSchema() + ".edek_records";
        this.stagingTable = config.stagingSchema() + ".edek_records";
        this.appsTable = config.targetAccessSchema() + ".app_registrations";
        at("source", "making the connection read-only");
        sourceDb.setReadOnly(true);
        at("target", "turning off auto-commit");
        targetDb.setAutoCommit(false);
        this.source = new SourceStore(sourceDb, config.source().cryptoSchema(), config.sourceLegacyKekName());
        this.staging = new StagingStore(targetDb, config.stagingSchema(), config.target().cryptoSchema(),
                config.targetAccessSchema());
    }

    // ---- analyze ----

    /** Reads A and checks B; writes nothing. {@code probe}: also one unwrap per A KEK version and one wrap per B KEK. */
    public int analyze(Scope scope, boolean probe) throws SQLException {
        List<String> problems = new ArrayList<>();
        at("source", "counting rows in " + sourceTable);
        List<SourceStore.Count> counts = source.counts(scope);
        long total = counts.stream().mapToLong(SourceStore.Count::rows).sum();
        out.println("analyze scope=" + scope + " rows=" + total);
        if (!scope.ids().isEmpty()) {
            at("source", "looking up the --ids in " + sourceTable);
            List<UUID> missing = source.missingIds(scope);
            for (UUID id : missing.subList(0, Math.min(missing.size(), FAILED_LIST_LIMIT))) {
                out.println("--ids edek_id=" + id + " is not in the source");
            }
            if (!missing.isEmpty()) {
                problems.add(missing.size() + " of the " + scope.ids().size() + " --ids aren't in the source");
            }
            long outside = scope.ids().size() - missing.size() - total;
            if (outside > 0) {
                out.println("--ids: " + outside + " edek_id(s) are in the source but outside the --kek/--app limits");
            }
        }
        out.println(String.format("%-30s %-34s %-9s %-30s %10s", "source KEK", "version", "status", "app_id", "rows"));
        Set<String> keks = new TreeSet<>();
        Set<String> owners = new TreeSet<>();
        for (SourceStore.Count c : counts) {
            out.println(String.format("%-30s %-34s %-9s %-30s %10d", c.kekName(), c.kekVersion(), c.rotationStatus(),
                    c.appId(), c.rows()));
            keks.add(c.kekName());
            owners.add(c.appId());
        }
        for (String kek : keks) {
            String to = config.targetKekFor(kek);
            out.println("kek_map " + kek + " -> " + (to == null ? "(none)" : to));
            if (to == null) {
                problems.add("source KEK '" + kek + "' has no kek-map entry");
            }
        }

        at("target", "checking owners in " + appsTable);
        Map<String, Boolean> registered = staging.registeredApps(owners);
        for (String app : owners) {
            Boolean active = registered.get(app);
            out.println("owner " + app + " in target app_registrations: "
                    + (active == null ? "MISSING" : active ? "active" : "inactive"));
            if (active == null) {
                problems.add("owner app_id '" + app + "' has no app_registrations row in the target");
            }
        }

        long idClashes = 0;
        long nameClashes = 0;
        UUID after = null;
        while (true) {
            at("source", "reading " + sourceTable);
            List<EdekRow> page = source.page(after, config.pageSize(), scope);
            if (page.isEmpty()) {
                break;
            }
            at("target", "checking edek_id and current_dek_name clashes in " + targetTable);
            idClashes += staging.idClashes(page).size();
            nameClashes += staging.currentNameClashes(page).size();
            after = page.get(page.size() - 1).edekId();
        }
        targetDb.rollback();
        out.println("target edek_id clashes=" + idClashes + " current_dek_name clashes=" + nameClashes);
        if (idClashes > 0) {
            problems.add(idClashes + " edek_id(s) already in the target's edek_records with another owner or fingerprint");
        }
        if (nameClashes > 0) {
            problems.add(nameClashes + " current_dek_name(s) already held by another row in the target");
        }

        if (probe) {
            at("source", "reading one row per KEK version from " + sourceTable);
            for (EdekRow sample : source.onePerKekVersion(scope)) {
                String kek = sample.resolvedKekName(config.sourceLegacyKekName());
                byte[] dek = null;
                try {
                    dek = sourceVault.unwrap(Base64.getDecoder().decode(sample.edekBlob()), kek, sample.kekVersion());
                    out.println("probe source unwrap " + kek + "/" + sample.kekVersion() + ": ok");
                } catch (RuntimeException e) {
                    String why = Errors.describe(e);
                    out.println("probe source unwrap " + kek + "/" + sample.kekVersion() + ": " + why);
                    problems.add("can't unwrap with source KEK " + kek + "/" + sample.kekVersion() + ": " + why);
                } finally {
                    zero(dek);
                }
            }
            for (String kek : keks) {
                String to = config.targetKekFor(kek);
                if (to != null) {
                    String result = probeTarget(to);
                    out.println("probe target wrap+unwrap " + to + ": " + result);
                    if (!result.equals("ok")) {
                        problems.add("target KEK " + to + ": " + result);
                    }
                }
            }
        }
        return finish("analyze", problems);
    }

    /** Wraps 32 random bytes with B's KEK and unwraps them back: proves wrap and unwrap access. */
    private String probeTarget(String kek) {
        byte[] test = new byte[32];
        new SecureRandom().nextBytes(test);
        byte[] back = null;
        try {
            KekVault.Wrapped w = targetVault.wrap(test, kek);
            back = targetVault.unwrap(w.edek(), kek, w.kekVersion());
            return MessageDigest.isEqual(test, back) ? "ok" : "the unwrapped bytes differ";
        } catch (RuntimeException e) {
            return Errors.describe(e);
        } finally {
            zero(test);
            zero(back);
        }
    }

    // ---- stage ----

    /** Copies A's rows into staging (no HSM call): new rows added, changed rows reset, gone rows marked removed. */
    public int stage(Scope scope) throws SQLException {
        List<String> unmapped = new ArrayList<>();
        at("source", "counting rows in " + sourceTable);
        for (SourceStore.Count c : source.counts(scope)) {
            if (config.targetKekFor(c.kekName()) == null && !unmapped.contains(c.kekName())) {
                unmapped.add(c.kekName());
            }
        }
        if (!unmapped.isEmpty()) {
            return finish("stage", List.of("source KEK(s) without a kek-map entry: " + String.join(", ", unmapped)));
        }
        at("target", "creating the staging table " + stagingTable);
        staging.init();
        targetDb.commit();

        String batch = newBatch();
        int added = 0;
        int changed = 0;
        int unchanged = 0;
        UUID after = null;
        while (true) {
            at("source", "reading " + sourceTable);
            List<EdekRow> page = source.page(after, config.pageSize(), scope);
            if (page.isEmpty()) {
                break;
            }
            at("target", "writing " + stagingTable);
            StagingStore.StageCounts c = staging.stage(page, batch, config.sourceLegacyKekName(),
                    config::targetKekFor);
            targetDb.commit();
            added += c.added();
            changed += c.changed();
            unchanged += c.unchanged();
            after = page.get(page.size() - 1).edekId();
            out.println("stage page rows=" + page.size() + " added=" + c.added() + " changed=" + c.changed());
        }
        at("target", "marking rows gone from the source in " + stagingTable);
        int removed = staging.markRemoved(batch, scope);
        targetDb.commit();
        out.println("stage done batch=" + batch + " scope=" + scope + " added=" + added + " changed=" + changed
                + " unchanged=" + unchanged + " removed=" + removed);
        return OK;
    }

    // ---- wrap ----

    /** For each staged or failed row: unwrap with A, wrap with B, store B's EDEK. */
    public int wrap(Scope scope) throws SQLException {
        return eachRow("wrap", Set.of("staged", "failed"), scope, (row, batch) -> {
            byte[] dek = null;
            try {
                dek = sourceVault.unwrap(Base64.getDecoder().decode(row.sourceBlob()), row.sourceKekName(),
                        row.sourceKekVersion());
                KekVault.Wrapped w = targetVault.wrap(dek, row.targetKekName());
                staging.setWrapped(row.edekId(), Base64.getEncoder().encodeToString(w.edek()), w.kekVersion(), batch);
                return null;
            } finally {
                zero(dek);
            }
        });
    }

    // ---- verify ----

    /** For each wrapped row: unwrap A's EDEK with A and B's EDEK with B; the two DEKs must be equal. */
    public int verify(Scope scope) throws SQLException {
        return eachRow("verify", Set.of("wrapped"), scope, (row, batch) -> {
            byte[] fromA = null;
            byte[] fromB = null;
            try {
                fromA = sourceVault.unwrap(Base64.getDecoder().decode(row.sourceBlob()), row.sourceKekName(),
                        row.sourceKekVersion());
                fromB = targetVault.unwrap(Base64.getDecoder().decode(row.targetBlob()), row.targetKekName(),
                        row.targetKekVersion());
                if (!MessageDigest.isEqual(fromA, fromB)) {
                    return "DekMismatch";
                }
                staging.setVerified(row.edekId(), batch);
                return null;
            } finally {
                zero(fromA);
                zero(fromB);
            }
        });
    }

    @FunctionalInterface
    private interface RowWork {
        /** null when the row succeeded, else the failure's type (the work sets the success state itself). */
        String apply(StagingStore.StagedRow row, String batch) throws SQLException;
    }

    private int eachRow(String command, Set<String> statuses, Scope scope, RowWork work) throws SQLException {
        String batch = newBatch();
        int done = 0;
        int failed = 0;
        int consecutive = 0;
        UUID after = null;
        Set<String> explained = new HashSet<>();
        while (true) {
            at("target", "reading " + stagingTable);
            List<StagingStore.StagedRow> page = staging.page(statuses, after, config.pageSize(), scope);
            if (page.isEmpty()) {
                break;
            }
            int pageDone = 0;
            int pageFailed = 0;
            for (StagingStore.StagedRow row : page) {
                String error;
                String reason = null;
                at("target", "updating edek_id=" + row.edekId() + " in " + stagingTable);
                try {
                    error = work.apply(row, batch);
                } catch (RuntimeException e) {
                    error = errorType(e);
                    reason = Errors.describe(e);
                }
                if (error == null) {
                    pageDone++;
                    consecutive = 0;
                } else {
                    staging.setFailed(row.edekId(), error, batch);
                    pageFailed++;
                    consecutive++;
                    out.println(command + " failed edek_id=" + row.edekId() + " source_kek=" + row.sourceKekName()
                            + " error=" + error);
                    // The full reason once per error type; the row itself keeps only the type.
                    if (reason != null && explained.add(error)) {
                        out.println(command + " reason for " + error + ": " + reason);
                    }
                    if (consecutive >= config.maxConsecutiveFailures()) {
                        targetDb.commit();
                        out.println(command + " stopped: " + consecutive + " failures in a row (an HSM or database "
                                + "may be down); rerun picks up where this stopped");
                        return STOPPED;
                    }
                }
            }
            targetDb.commit();
            done += pageDone;
            failed += pageFailed;
            after = page.get(page.size() - 1).edekId();
            out.println(command + " page rows=" + page.size() + " ok=" + pageDone + " failed=" + pageFailed);
        }
        out.println(command + " done batch=" + batch + " scope=" + scope + " ok=" + done + " failed=" + failed);
        return failed == 0 ? OK : PROBLEMS;
    }

    // ---- report ----

    /** Counts per source KEK, owner and state; the failed rows; whether the scope is ready to promote. */
    public int report(Scope scope) throws SQLException {
        at("target", "reading " + stagingTable);
        List<StagingStore.GroupCount> counts = staging.counts(scope);
        Map<String, Map<String, Long>> perKek = new TreeMap<>();
        out.println("report scope=" + scope);
        out.println(String.format("%-30s %-30s %-9s %10s", "source KEK", "app_id", "status", "rows"));
        for (StagingStore.GroupCount c : counts) {
            out.println(String.format("%-30s %-30s %-9s %10d", c.sourceKekName(), c.appId(), c.status(), c.rows()));
            perKek.computeIfAbsent(c.sourceKekName(), k -> new TreeMap<>()).merge(c.status(), c.rows(), Long::sum);
        }
        boolean ready = !counts.isEmpty();
        for (Map.Entry<String, Map<String, Long>> e : perKek.entrySet()) {
            Map<String, Long> s = e.getValue();
            long notReady = s.entrySet().stream().filter(x -> !x.getKey().equals("verified")
                    && !x.getKey().equals("promoted")).mapToLong(Map.Entry::getValue).sum();
            ready &= notReady == 0;
            out.println("kek " + e.getKey() + " " + s + (notReady == 0 ? " ready" : " not ready"));
        }
        List<StagingStore.Failed> failed = staging.failed(scope, FAILED_LIST_LIMIT);
        for (StagingStore.Failed f : failed) {
            out.println("failed edek_id=" + f.edekId() + " source_kek=" + f.sourceKekName() + " app_id=" + f.appId()
                    + " error=" + f.errorType());
        }
        if (failed.size() == FAILED_LIST_LIMIT) {
            out.println("(first " + FAILED_LIST_LIMIT + " failed rows shown)");
        }
        if (!scope.ids().isEmpty()) {
            List<StagingStore.Listed> listed = staging.listed(scope, LISTED_LIMIT);
            long notStaged = 0;
            for (StagingStore.Listed l : listed) {
                if (l.status() == null) {
                    notStaged++;
                    out.println("id edek_id=" + l.edekId() + " not staged (or outside the --kek/--app limits)");
                } else {
                    out.println("id edek_id=" + l.edekId() + " source_kek=" + l.sourceKekName() + " app_id="
                            + l.appId() + " status=" + l.status()
                            + (l.errorType() == null ? "" : " error=" + l.errorType()));
                }
            }
            if (listed.size() == LISTED_LIMIT) {
                out.println("(first " + LISTED_LIMIT + " --ids shown)");
            }
            if (notStaged > 0) {
                ready = false;
                out.println(notStaged + " of the --ids aren't staged: run stage with the same --ids");
            }
        }
        targetDb.rollback();
        out.println("ready to promote: " + (ready ? "yes" : "no"));
        return OK;
    }

    // ---- promote ----

    public int promote(Scope scope) throws SQLException {
        StagingStore.PromoteResult r;
        at("target", "promoting " + stagingTable + " into " + targetTable);
        try {
            r = staging.promote(scope);
        } catch (SQLException e) {
            targetDb.rollback();
            throw e;
        }
        if (r.refused()) {
            targetDb.rollback();
            List<String> problems = new ArrayList<>();
            if (!r.blockers().isEmpty()) {
                problems.add("rows not verified: " + r.blockers());
            }
            if (r.clashes() > 0) {
                problems.add(r.clashes() + " verified row(s) whose edek_id is already in the target's edek_records");
            }
            return finish("promote", problems);
        }
        targetDb.commit();
        out.println("promote done scope=" + scope + " updated=" + r.updated() + " inserted=" + r.inserted()
                + "; restart the target's hsm-core pods, then decrypt sample tokens through it");
        if (!scope.isAll()) {
            out.println("promote: only this scope is in the target's edek_records; the target can't decrypt tokens "
                    + "of rows not promoted yet, so promote the rest before it takes traffic");
        }
        return OK;
    }

    // ---- reset ----

    public int reset(Scope scope, boolean confirmed) throws SQLException {
        if (!confirmed) {
            out.println("reset deletes the staging rows for scope=" + scope + "; add --yes to do it");
            return USAGE;
        }
        at("target", "deleting rows from " + stagingTable);
        staging.init();
        int n = staging.reset(scope);
        targetDb.commit();
        out.println("reset done scope=" + scope + " deleted=" + n);
        return OK;
    }

    // ---- helpers ----

    private void at(String side, String what) {
        this.stepSide = side;
        this.step = what;
    }

    /** What the command was doing, e.g. {@code "target database, checking owners in hsm_access.app_registrations"}. */
    public String step() {
        return stepSide + " database, " + step;
    }

    /** "source" or "target": the database of {@link #step()}. */
    public String stepSide() {
        return stepSide;
    }

    private int finish(String command, List<String> problems) {
        for (String p : new LinkedHashSet<>(problems)) {
            out.println(command + " problem: " + p);
        }
        out.println(command + (problems.isEmpty() ? " ok" : " found " + problems.size() + " problem(s)"));
        return problems.isEmpty() ? OK : PROBLEMS;
    }

    private static String newBatch() {
        return Instant.now().toString() + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    static void zero(byte[] b) {
        if (b != null) {
            Arrays.fill(b, (byte) 0);
        }
    }

    /** The error's type, and an HTTP status if it has one. Never its message: that can carry request details. */
    static String errorType(Throwable e) {
        if (e instanceof HttpResponseException h && h.getResponse() != null) {
            return e.getClass().getSimpleName() + "(" + h.getResponse().getStatusCode() + ")";
        }
        return e.getClass().getSimpleName();
    }
}
