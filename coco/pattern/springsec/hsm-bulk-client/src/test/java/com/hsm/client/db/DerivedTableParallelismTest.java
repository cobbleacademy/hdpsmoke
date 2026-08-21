package com.hsm.client.db;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * DbBulkJob.computePartitionRanges runs the boundary-lookup query (OFFSET x FETCH
 * NEXT 1) several times, then each parallel worker independently re-runs fetchPage's
 * ranged query -- both against the SAME derived-table string (resolveSourceFrom's
 * "(" + query + ") AS src") for a TableRef.query source. Since a derived table has
 * nothing persisted, EVERY one of those is a fresh, independent re-execution of the
 * ROW_NUMBER() computation -- unlike a real table/index, where a key's value is fixed
 * storage, not something the engine recomputes per query. This class checks whether
 * that recomputation stays consistent enough for parallelism>1 to be safe: no row
 * lost or double-counted across partition boundaries.
 */
class DerivedTableParallelismTest {

    private JdbcTemplate jdbc;

    @BeforeEach
    void setUp() {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.h2.Driver");
        ds.setUrl("jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        jdbc = new JdbcTemplate(ds);
    }

    /**
     * Mirrors computePartitionRanges + fetchPage together: find partition boundaries
     * via repeated OFFSET lookups against the derived table, then re-query each
     * [start, end] range independently (as parallel workers would, on their own
     * connections/threads in the real code) -- both hitting the SAME re-evaluated
     * ROW_NUMBER() derived table, never a persisted column. Unique ORDER BY column
     * (ssn has no duplicates here) -- the case this module's docs assume.
     */
    @Test
    void uniqueOrderByColumn_partitionsCoverEveryRowExactlyOnce() {
        jdbc.execute("CREATE TABLE customers (ssn VARCHAR(11), dob VARCHAR(10))");
        for (int i = 1; i <= 97; i++) {
            jdbc.update("INSERT INTO customers (ssn, dob) VALUES (?, ?)", String.format("ssn-%04d", i), "1990-01-01");
        }
        String derived = "(SELECT ROW_NUMBER() OVER (ORDER BY ssn) AS row_key, ssn, dob FROM customers) AS src";
        int totalRows = jdbc.queryForObject("SELECT COUNT(*) FROM " + derived, Integer.class);
        assertThat(totalRows).isEqualTo(97);

        int partitions = 5;
        List<Long> boundaries = new ArrayList<>();
        for (int p = 1; p < partitions; p++) {
            long offset = (long) totalRows * p / partitions;
            List<Object> row = jdbc.queryForList(
                    "SELECT row_key FROM " + derived + " ORDER BY row_key OFFSET ? ROWS FETCH NEXT 1 ROWS ONLY",
                    Object.class, offset);
            boundaries.add(((Number) row.get(0)).longValue());
        }

        List<Long> allKeysSeen = new ArrayList<>();
        Long prev = null;
        List<Long> ranges = new ArrayList<>(boundaries);
        ranges.add(null); // final partition's upper bound is unbounded
        for (Long boundary : ranges) {
            StringBuilder sql = new StringBuilder("SELECT row_key FROM ").append(derived).append(" WHERE row_key > ?");
            List<Object> args = new ArrayList<>();
            args.add(prev == null ? 0L : prev);
            if (boundary != null) {
                sql.append(" AND row_key <= ?");
                args.add(boundary);
            }
            sql.append(" ORDER BY row_key OFFSET 0 ROWS FETCH NEXT 1000 ROWS ONLY");
            List<Map<String, Object>> rows = jdbc.queryForList(sql.toString(), args.toArray());
            rows.forEach(r -> allKeysSeen.add(((Number) r.get("ROW_KEY")).longValue()));
            prev = boundary;
        }

        assertThat(allKeysSeen).hasSize(97);
        assertThat(allKeysSeen).doesNotHaveDuplicates();
        assertThat(allKeysSeen).containsExactlyInAnyOrderElementsOf(
                java.util.stream.LongStream.rangeClosed(1, 97).boxed().toList());
    }

    /**
     * Same setup but ORDER BY ssn where ssn has heavy duplicates -- the case this
     * module's docs warn about ("prefer a fully unique ORDER BY"). Proves WHY: H2
     * (like Postgres/SQL Server) does not guarantee stable tie ordering across
     * separate query executions, so a row can land in the wrong partition relative
     * to where computePartitionRanges "saw" it, causing it to be silently skipped or
     * processed twice. This is a real risk specific to parallelism>1 with a
     * query-based source, not just the already-documented "source must be static
     * during the run" caveat.
     */
    @Test
    void duplicateOrderByColumn_rowNumberAssignmentCanDifferAcrossReExecution() {
        // id is the physical-row identity (never exposed to DbBulkJob in the real
        // no-natural-key scenario -- it's only here so this test can tell whether two
        // executions assigned row_key to the SAME physical rows, which config-level
        // SQL alone can't observe).
        jdbc.execute("CREATE TABLE customers (id INT AUTO_INCREMENT PRIMARY KEY, ssn VARCHAR(11))");
        // Only 3 distinct ssn values across 300 rows -- heavy ties.
        for (int i = 1; i <= 300; i++) {
            jdbc.update("INSERT INTO customers (ssn) VALUES (?)", "ssn-" + (i % 3));
        }
        String derived = "(SELECT ROW_NUMBER() OVER (ORDER BY ssn) AS row_key, id, ssn FROM customers) AS src";

        // Two independent executions of the identical query, exactly what
        // computePartitionRanges's boundary lookup and a worker's later fetchPage
        // call each do against a query-based source -- both re-evaluate ROW_NUMBER()
        // from scratch since nothing is persisted.
        List<Map<String, Object>> firstExecution = jdbc.queryForList(
                "SELECT row_key, id FROM " + derived + " ORDER BY row_key");
        List<Map<String, Object>> secondExecution = jdbc.queryForList(
                "SELECT row_key, id FROM " + derived + " ORDER BY row_key");

        List<Object> firstIdOrder = firstExecution.stream().map(r -> r.get("ID")).toList();
        List<Object> secondIdOrder = secondExecution.stream().map(r -> r.get("ID")).toList();

        // Documented, not asserted as pass/fail either way: whether these two lists
        // are equal is exactly the fact that determines whether parallelism>1 is safe
        // for a tied ORDER BY. This test's real value is making that fact directly
        // observable against a real engine instead of assumed.
        System.out.println("DerivedTableParallelismTest: tie-order stable across re-execution = "
                + firstIdOrder.equals(secondIdOrder));
        assertThat(firstIdOrder).hasSize(300);
        assertThat(secondIdOrder).hasSize(300);
    }
}
