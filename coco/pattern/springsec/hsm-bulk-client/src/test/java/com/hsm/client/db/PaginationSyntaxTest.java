package com.hsm.client.db;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Live proof, against a real database (H2), that the exact ANSI SQL:2008 OFFSET/FETCH
 * text DbBulkJob.fetchPage/computePartitionRanges now generate (replacing Postgres's
 * LIMIT shorthand) is valid, portable SQL -- see DbBulkJob's class javadoc for why this
 * form needed no per-dialect branching, unlike CheckpointStore.
 *
 * <p>Doesn't exercise DbBulkJob itself (its private fetchPage/computePartitionRanges
 * aren't independently invokable without a full SvcClient/DEK-issuance stub) -- tests
 * the literal SQL text those methods build, directly, which is the actually novel and
 * risk-bearing part of this change. H2 chosen deliberately: it's not one of the three
 * dialects DbBulkJob's own source/target ever targets (see CheckpointStoreTest for
 * those), but it's a real, independent SQL engine that also implements the SQL:2008
 * OFFSET/FETCH clause -- a second real engine agreeing with Postgres/SQL
 * Server/Oracle's own documented syntax is meaningful corroboration that this is
 * genuinely standard, not something that happens to work by coincidence on one
 * vendor.
 */
class PaginationSyntaxTest {

    private JdbcTemplate jdbc;

    @BeforeEach
    void setUp() {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.h2.Driver");
        ds.setUrl("jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        jdbc = new JdbcTemplate(ds);
        jdbc.execute("CREATE TABLE src (id BIGINT PRIMARY KEY, val VARCHAR(50))");
        for (long i = 1; i <= 25; i++) {
            jdbc.update("INSERT INTO src (id, val) VALUES (?, ?)", i, "row-" + i);
        }
    }

    /** Mirrors fetchPage's row-count-cap clause: OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY. */
    @Test
    void fetchPageStyleQuery_returnsExactlyRequestedPageSize_inKeyOrder() {
        List<Map<String, Object>> page = jdbc.queryForList(
                "SELECT id, val FROM src ORDER BY id OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY", 10);

        assertThat(page).hasSize(10);
        assertThat(page.get(0).get("ID")).isEqualTo(1L);
        assertThat(page.get(9).get("ID")).isEqualTo(10L);
    }

    /** Mirrors fetchPage's continuation form: keyset WHERE id > ? combined with the same FETCH NEXT cap. */
    @Test
    void fetchPageStyleQuery_withKeysetContinuation_resumesPastLastKey() {
        List<Map<String, Object>> page = jdbc.queryForList(
                "SELECT id, val FROM src WHERE id > ? ORDER BY id OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY",
                15L, 10);

        assertThat(page).hasSize(10);
        assertThat(page.get(0).get("ID")).isEqualTo(16L);
        assertThat(page.get(9).get("ID")).isEqualTo(25L);
    }

    /** Mirrors fetchPage's uptoInclusive bound (used by parallel partition workers): id > ? AND id <= ?. */
    @Test
    void fetchPageStyleQuery_withUptoInclusiveBound_stopsAtPartitionEnd() {
        List<Map<String, Object>> page = jdbc.queryForList(
                "SELECT id, val FROM src WHERE id > ? AND id <= ? ORDER BY id OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY",
                5L, 12L, 100);

        assertThat(page).hasSize(7);
        assertThat(page.get(0).get("ID")).isEqualTo(6L);
        assertThat(page.get(6).get("ID")).isEqualTo(12L);
    }

    /** Mirrors computePartitionRanges's boundary-key lookup: OFFSET ? ROWS FETCH NEXT 1 ROWS ONLY. */
    @Test
    void boundaryLookupStyleQuery_returnsCorrectSingleRowAtOffset() {
        List<Object> row = jdbc.queryForList(
                "SELECT id FROM src ORDER BY id OFFSET ? ROWS FETCH NEXT 1 ROWS ONLY", Object.class, 12);

        assertThat(row).hasSize(1);
        // 0-indexed offset 12 -> the 13th row -> id=13.
        assertThat(row.get(0)).isEqualTo(13L);
    }

    @Test
    void fetchPageStyleQuery_pastEndOfTable_returnsEmpty() {
        List<Map<String, Object>> page = jdbc.queryForList(
                "SELECT id, val FROM src WHERE id > ? ORDER BY id OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY",
                25L, 10);

        assertThat(page).isEmpty();
    }

    /**
     * Mirrors DbBulkJob.resolveSourceFrom's derived-table form for TableRef.query --
     * "(" + query + ") AS src" used as fetchPage's FROM target. Proves a source with
     * no natural sortable key can compute one inline (ROW_NUMBER(), no CREATE VIEW
     * needed) and that keyset pagination against it behaves identically to pagination
     * against a plain table -- the derived table has no primary key/index at all here,
     * unlike src's real id column, so this also proves the pattern doesn't secretly
     * depend on one.
     */
    @Test
    void derivedTableSource_withRowNumberKey_paginatesCorrectly() {
        jdbc.execute("CREATE TABLE no_natural_key (ssn VARCHAR(11), dob VARCHAR(10))");
        for (long i = 1; i <= 25; i++) {
            jdbc.update("INSERT INTO no_natural_key (ssn, dob) VALUES (?, ?)", "ssn-" + i, "1990-01-01");
        }
        String derivedTable = "(SELECT ROW_NUMBER() OVER (ORDER BY ssn) AS row_key, ssn, dob FROM no_natural_key) AS src";

        List<Map<String, Object>> firstPage = jdbc.queryForList(
                "SELECT row_key, ssn, dob FROM " + derivedTable
                        + " ORDER BY row_key OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY", 10);
        assertThat(firstPage).hasSize(10);
        assertThat(firstPage.get(0).get("ROW_KEY")).isEqualTo(1L);
        assertThat(firstPage.get(9).get("ROW_KEY")).isEqualTo(10L);

        Object lastKey = firstPage.get(9).get("ROW_KEY");
        List<Map<String, Object>> secondPage = jdbc.queryForList(
                "SELECT row_key, ssn, dob FROM " + derivedTable
                        + " WHERE row_key > ? ORDER BY row_key OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY",
                lastKey, 100);
        assertThat(secondPage).hasSize(15);
        assertThat(secondPage.get(0).get("ROW_KEY")).isEqualTo(11L);
        assertThat(secondPage.get(14).get("ROW_KEY")).isEqualTo(25L);
    }

    /** Mirrors DbBulkJob.insertRows when keyColumn is null (config.skipKeyColumnOnTarget=true): no leading key column/placeholder at all. */
    @Test
    void insertStyleStatement_withoutKeyColumn_omitsLeadingKeyEntirely() {
        jdbc.execute("CREATE TABLE target_no_key (ssn_ciphertext VARCHAR(200), dob_ciphertext VARCHAR(200))");

        jdbc.update("INSERT INTO target_no_key (ssn_ciphertext, dob_ciphertext) VALUES (?, ?)", "enc-ssn", "enc-dob");

        List<Map<String, Object>> rows = jdbc.queryForList("SELECT * FROM target_no_key");
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).keySet()).containsExactlyInAnyOrder("SSN_CIPHERTEXT", "DOB_CIPHERTEXT");
    }
}
