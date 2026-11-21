package com.hsm.client.deid;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/**
 * What a run gives the transforms besides the profile: the clock, the run id, and the
 * keys and surrogate table when the profile needs them.
 *
 * @param clock              the run date: the {@code birth-year} cut-off is taken from it
 * @param runId              recorded on new surrogate rows
 * @param keys               needed only for {@code tokenize} or {@code surrogate}
 * @param surrogateKeyEdekId the surrogate-table key ({@code client.deid.surrogate.key-edek-id}); only for {@code surrogate}
 * @param surrogateStore     only for {@code surrogate}
 * @param restrictedZip3     ZIP3s written as {@code 000}; null for the shipped HHS list
 */
public record TransformContext(Clock clock, String runId, TokenKeyProvider keys, UUID surrogateKeyEdekId,
                               SurrogateStore surrogateStore, Set<String> restrictedZip3) {

    private static final String RESTRICTED_ZIP3_RESOURCE = "/com/hsm/client/deid/restricted-zip3.txt";

    public TransformContext {
        if (clock == null) {
            clock = Clock.systemUTC();
        }
        restrictedZip3 = restrictedZip3 == null ? defaultRestrictedZip3() : Set.copyOf(restrictedZip3);
    }

    /** A context for profiles that use no keys and no surrogate table. */
    public static TransformContext withoutKeys(Clock clock, String runId) {
        return new TransformContext(clock, runId, null, null, null, null);
    }

    /**
     * The HHS list of ZIP3 areas with 20,000 people or fewer, shipped as a reviewed
     * resource. One ZIP3 per line; {@code #} starts a comment.
     */
    public static Set<String> defaultRestrictedZip3() {
        try (InputStream in = TransformContext.class.getResourceAsStream(RESTRICTED_ZIP3_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("missing resource " + RESTRICTED_ZIP3_RESOURCE);
            }
            Set<String> zips = new TreeSet<>();
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                int hash = line.indexOf('#');
                String zip = (hash >= 0 ? line.substring(0, hash) : line).trim();
                if (!zip.isEmpty()) {
                    if (!zip.matches("\\d{3}")) {
                        throw new IllegalStateException("bad ZIP3 in " + RESTRICTED_ZIP3_RESOURCE + ": " + zip);
                    }
                    zips.add(zip);
                }
            }
            return Set.copyOf(zips);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
