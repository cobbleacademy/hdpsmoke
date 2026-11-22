package com.hsm.client.deidjob;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.hsm.client.deid.ColumnTransform.Method;
import com.hsm.client.deid.DeidPipeline;
import com.hsm.client.deid.DeidProfile;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * {@code <file>.manifest.json}, written beside the CSV (DEIDENTIFICATION.md section
 * 10.2). Counts, settings and hashes only: never a value, token or surrogate. It stays
 * with the data owner; the transfer process sends only the CSV.
 */
public final class DeidManifest {

    private static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private DeidManifest() {
    }

    /** What the job knows about one run, beside the pipeline's own report. */
    public record Run(String runId, String profileSha256, String triggeredBy, Instant startedAt, Instant completedAt,
                      UUID surrogateKeyEdekId, List<String> sourceColumns, long rowsSkipped, String outputFile,
                      String outputSha256, boolean dryRun) {
    }

    public static Map<String, Object> build(DeidProfile profile, DeidPipeline pipeline, Run run) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("run_id", run.runId());
        if (run.dryRun()) {
            m.put("dry_run", true);
        }

        Map<String, Object> p = new LinkedHashMap<>();
        p.put("id", profile.id());
        p.put("version", profile.version());
        p.put("sha256", run.profileSha256());
        p.put("vendor", profile.vendor());
        p.put("hipaa_basis", profile.basis().label());
        if (profile.expertReport() != null) {
            p.put("expert_report", profile.expertReport());
        }
        m.put("profile", p);
        m.put("triggered_by", run.triggeredBy());
        m.put("started_at", String.valueOf(run.startedAt()));
        m.put("completed_at", run.completedAt() == null ? null : run.completedAt().toString());

        boolean tokenize = uses(profile, Method.TOKENIZE);
        boolean surrogate = uses(profile, Method.SURROGATE);
        Map<String, Object> keys = new LinkedHashMap<>();
        keys.put("token", tokenize ? Map.of("edek_id", String.valueOf(profile.tokenKeyEdekId()),
                "epoch", profile.tokenEpoch()) : null);
        keys.put("surrogate", surrogate ? Map.of("edek_id", String.valueOf(run.surrogateKeyEdekId())) : null);
        m.put("keys", keys);

        List<Map<String, Object>> fields = new ArrayList<>();
        long newSurrogates = 0;
        long existingSurrogates = 0;
        List<String> decrypted = new ArrayList<>();
        for (DeidPipeline.FieldReport r : pipeline.report()) {
            Map<String, Object> f = new LinkedHashMap<>();
            f.put("source", r.source());
            f.put("output", r.output());
            if (r.phi() != null) {
                f.put("phi", r.phi());
            }
            f.put("method", r.method());
            r.settings().forEach(f::putIfAbsent);
            if (r.decrypted()) {
                f.put("decrypted", true);
                decrypted.add(r.source());
            }
            f.put("values", r.values());
            f.put("empty", r.empty());
            r.counters().forEach((k, v) -> {
                if (!"decrypted".equals(k)) {
                    f.putIfAbsent(k, v);
                }
            });
            if (r.method().startsWith(Method.SURROGATE.label() + "@")) {
                newSurrogates += r.counters().getOrDefault("new", 0L);
                existingSurrogates += r.counters().getOrDefault("existing", 0L);
            }
            fields.add(f);
        }
        m.put("fields", fields);
        m.put("dropped_by_default", pipeline.droppedByDefault(run.sourceColumns()));
        if (surrogate) {
            m.put("surrogates", Map.of("new", newSurrogates, "existing", existingSurrogates));
        }
        if (!decrypted.isEmpty()) {
            Map<String, Object> d = new LinkedHashMap<>();
            d.put("fields", decrypted);
            d.put("distinct_edek_ids", pipeline.decryptedEdekIds().size());
            m.put("decrypt", d);
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("file", run.outputFile());
        out.put("rows_in", pipeline.rows() + run.rowsSkipped());
        out.put("rows_out", pipeline.rows());
        out.put("rows_skipped", run.rowsSkipped());
        out.put("sha256", run.outputSha256());
        m.put("output", out);
        return m;
    }

    private static boolean uses(DeidProfile profile, Method method) {
        return profile.fields().stream().anyMatch(f -> f.method() == method);
    }

    public static String toJson(Map<String, Object> manifest) {
        try {
            return JSON.writeValueAsString(manifest);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Writes {@code <target>.part}, then renames it to {@code target}. */
    public static void write(Path target, Map<String, Object> manifest) {
        Path part = target.resolveSibling(target.getFileName() + ".part");
        try {
            Files.writeString(part, toJson(manifest) + "\n");
            try {
                Files.move(part, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(part, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            try {
                Files.deleteIfExists(part);
            } catch (IOException ignored) {
                // reporting the first failure
            }
            throw new UncheckedIOException("writing " + target.getFileName() + " failed", e);
        }
    }
}
