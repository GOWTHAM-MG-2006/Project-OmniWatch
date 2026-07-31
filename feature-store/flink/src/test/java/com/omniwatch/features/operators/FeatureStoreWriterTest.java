/*
 * OmniWatch — Windowing Layer / Feature Store
 * Component: FeatureStoreWriterTest
 * Phase: 4
 * Purpose: Unit tests for FeatureStoreWriter — DDL schema validation, retry/drop
 *          semantics, and flush-trigger logic. Uses java.lang.reflect.Proxy for
 *          a fake Connection (no Mockito in test classpath).
 * Inputs: FeatureStoreWriter static methods + reflection for transient state
 * Outputs: JUnit 5 assertions on DDL content, dropped-batches counter, buffer size
 */
package com.omniwatch.features.operators;

import com.omniwatch.features.models.FeatureVector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FeatureStoreWriterTest {

    @BeforeEach
    void resetCounter() {
        FeatureStoreWriter.resetDroppedBatches();
    }

    // ----------------------------------------------------------------
    // Helper: build a FeatureVector with all fields populated
    // ----------------------------------------------------------------

    private static FeatureVector sampleVector() {
        FeatureVector fv = new FeatureVector();
        fv.setEntityId("gcp:API_NODE/web-1");
        fv.setWindowStart("2026-07-31T10:00:00Z");
        fv.setWindowEnd("2026-07-31T10:05:00Z");
        fv.setWindowSize("5m");
        fv.setLatencyP50(12.5);
        fv.setLatencyP95(45.0);
        fv.setLatencyP99(120.0);
        fv.setLatencyAvg(18.3);
        fv.setLatencyMin(2.1);
        fv.setLatencyMax(250.0);
        fv.setErrorRate(0.02);
        fv.setRequestVolume(15000L);
        fv.setFeatureVersion(1);
        fv.setTtl(90);
        fv.setTimestamp("2026-07-31T10:05:00Z");
        return fv;
    }

    // ----------------------------------------------------------------
    // Helper: fake Connection that always throws on prepareStatement
    // ----------------------------------------------------------------

    private static Connection alwaysFailingConnection() {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, args) -> {
                    throw new SQLException("Simulated ClickHouse write failure");
                });
    }

    // ----------------------------------------------------------------
    // Helper: fake Connection that succeeds on prepareStatement
    // (returns a no-op PreparedStatement proxy)
    // ----------------------------------------------------------------

    private static Connection alwaysSucceedingConnection() {
        Object preparedStmtProxy = Proxy.newProxyInstance(
                java.sql.PreparedStatement.class.getClassLoader(),
                new Class<?>[]{java.sql.PreparedStatement.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "executeBatch": return new int[]{1};
                        case "addBatch":     return null;
                        case "close":        return null;
                        default:             return null;
                    }
                });

        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, args) -> {
                    if ("prepareStatement".equals(method.getName())) {
                        return preparedStmtProxy;
                    }
                    if ("isClosed".equals(method.getName())) return false;
                    if ("close".equals(method.getName())) return null;
                    return null;
                });
    }

    // ================================================================
    // Test 1: DDL contains all 15 snake_case columns + engine keywords
    // ================================================================

    @Test
    void ddlContainsAllRequiredColumnsAndEngineKeywords() {
        String ddl = FeatureStoreWriter.DDL;

        // All 15 snake_case column names
        assertTrue(ddl.contains("entity_id String"),       "missing entity_id");
        assertTrue(ddl.contains("window_start DateTime"),  "missing window_start");
        assertTrue(ddl.contains("window_end DateTime"),    "missing window_end");
        assertTrue(ddl.contains("window_size String"),     "missing window_size");
        assertTrue(ddl.contains("latency_p50 Float64"),   "missing latency_p50");
        assertTrue(ddl.contains("latency_p95 Float64"),   "missing latency_p95");
        assertTrue(ddl.contains("latency_p99 Float64"),   "missing latency_p99");
        assertTrue(ddl.contains("latency_avg Float64"),   "missing latency_avg");
        assertTrue(ddl.contains("latency_min Float64"),   "missing latency_min");
        assertTrue(ddl.contains("latency_max Float64"),   "missing latency_max");
        assertTrue(ddl.contains("error_rate Float64"),    "missing error_rate");
        assertTrue(ddl.contains("request_volume UInt64"), "missing request_volume");
        assertTrue(ddl.contains("feature_version UInt32"), "missing feature_version");
        assertTrue(ddl.contains("ttl UInt32"),            "missing ttl");
        assertTrue(ddl.contains("timestamp DateTime"),    "missing timestamp");

        // Engine and partitioning
        assertTrue(ddl.contains("MergeTree"),                    "missing MergeTree engine");
        assertTrue(ddl.contains("toYYYYMMDD(timestamp)"),       "missing toYYYYMMDD partition");
        assertTrue(ddl.contains("INTERVAL 90 DAY"),             "missing 90-day TTL");
        assertTrue(ddl.contains("PARTITION BY"),                "missing PARTITION BY");
        assertTrue(ddl.contains("ORDER BY (entity_id, window_start)"), "missing ORDER BY");
        assertTrue(ddl.contains("CREATE TABLE IF NOT EXISTS"),  "missing IF NOT EXISTS");
    }

    // ================================================================
    // Test 2: INSERT_SQL has exactly 15 placeholders
    // ================================================================

    @Test
    void insertSqlHasFifteenPlaceholders() {
        String sql = FeatureStoreWriter.INSERT_SQL;
        int questionMarks = 0;
        for (char c : sql.toCharArray()) {
            if (c == '?') questionMarks++;
        }
        assertEquals(15, questionMarks, "INSERT must have exactly 15 ? placeholders");
        assertTrue(sql.startsWith("INSERT INTO feature_vectors"), "must start with INSERT INTO");
    }

    // ================================================================
    // Test 3: Retry logic — failing connection drops batch after 3 retries
    // ================================================================

    @Test
    void droppedAfterRetriesOnFailure() {
        // Shorten retry delays to make the test fast (array is mutable even though
        // the reference is final).
        long[] saved = FeatureStoreWriter.RETRY_DELAYS_MS.clone();
        try {
            FeatureStoreWriter.RETRY_DELAYS_MS[0] = 0L;
            FeatureStoreWriter.RETRY_DELAYS_MS[1] = 0L;
            FeatureStoreWriter.RETRY_DELAYS_MS[2] = 0L;

            List<FeatureVector> batch = Arrays.asList(sampleVector(), sampleVector());
            Connection failingConn = alwaysFailingConnection();

            // Should NOT throw — retries are caught internally
            assertDoesNotThrow(() ->
                    FeatureStoreWriter.executeBatchInsert(failingConn, batch));

            assertEquals(1, FeatureStoreWriter.getDroppedBatches(),
                    "exactly 1 batch should be dropped after exhausting retries");
        } finally {
            System.arraycopy(saved, 0, FeatureStoreWriter.RETRY_DELAYS_MS, 0, 3);
        }
    }

    // ================================================================
    // Test 4: Successful insert does NOT increment dropped counter
    // ================================================================

    @Test
    void successfulInsertDoesNotDrop() {
        List<FeatureVector> batch = Arrays.asList(sampleVector(), sampleVector());
        Connection successConn = alwaysSucceedingConnection();

        assertDoesNotThrow(() ->
                FeatureStoreWriter.executeBatchInsert(successConn, batch));

        assertEquals(0, FeatureStoreWriter.getDroppedBatches(),
                "successful insert must not increment dropped counter");
    }

    // ================================================================
    // Test 5: Null / empty inputs are silently ignored
    // ================================================================

    @Test
    void nullConnectionIsIgnored() {
        assertDoesNotThrow(() ->
                FeatureStoreWriter.executeBatchInsert(null, Arrays.asList(sampleVector())));
        assertEquals(0, FeatureStoreWriter.getDroppedBatches());
    }

    @Test
    void emptyBatchIsIgnored() {
        Connection conn = alwaysSucceedingConnection();
        assertDoesNotThrow(() ->
                FeatureStoreWriter.executeBatchInsert(conn, new ArrayList<>()));
        assertEquals(0, FeatureStoreWriter.getDroppedBatches());
    }

    // ================================================================
    // Test 6: Flush triggers at 100 rows (size-based)
    // ================================================================

    @Test
    void flushTriggersAt100Rows() throws Exception {
        FeatureStoreWriter writer = new FeatureStoreWriter("localhost", 8123, "omniwatch");

        // Initialize transient fields via reflection
        Field bufferField = FeatureStoreWriter.class.getDeclaredField("buffer");
        bufferField.setAccessible(true);
        List<FeatureVector> buffer = new ArrayList<>();
        bufferField.set(writer, buffer);

        Field lastFlushField = FeatureStoreWriter.class.getDeclaredField("lastFlushMs");
        lastFlushField.setAccessible(true);
        lastFlushField.set(writer, System.currentTimeMillis());

        // connection is null — flush() will clear buffer, executeBatchInsert returns early

        // Add 99 items — should NOT flush
        for (int i = 0; i < 99; i++) {
            writer.invoke(sampleVector(), null);
        }
        assertEquals(99, buffer.size(), "buffer should hold 99 rows before threshold");

        // Add 100th item — size >= BATCH_SIZE triggers flush → buffer cleared
        writer.invoke(sampleVector(), null);
        assertEquals(0, buffer.size(), "buffer should be empty after flush at 100 rows");
    }

    // ================================================================
    // Test 7: Flush triggers after 1-second interval (time-based)
    // ================================================================

    @Test
    void flushTriggersAfterTimeInterval() throws Exception {
        FeatureStoreWriter writer = new FeatureStoreWriter("localhost", 8123, "omniwatch");

        Field bufferField = FeatureStoreWriter.class.getDeclaredField("buffer");
        bufferField.setAccessible(true);
        List<FeatureVector> buffer = new ArrayList<>();
        bufferField.set(writer, buffer);

        Field lastFlushField = FeatureStoreWriter.class.getDeclaredField("lastFlushMs");
        lastFlushField.setAccessible(true);
        // Set lastFlushMs to 2 seconds ago so the time check triggers
        lastFlushField.set(writer, System.currentTimeMillis() - 2000L);

        // Add 1 item — time-based flush should trigger (even though < 100 rows)
        writer.invoke(sampleVector(), null);
        assertEquals(0, buffer.size(),
                "buffer should be empty after time-interval flush with 1 row");
    }

    // ================================================================
    // Test 8: close() flushes remaining buffer
    // ================================================================

    @Test
    void closeFlushesRemainingBuffer() throws Exception {
        FeatureStoreWriter writer = new FeatureStoreWriter("localhost", 8123, "omniwatch");

        Field bufferField = FeatureStoreWriter.class.getDeclaredField("buffer");
        bufferField.setAccessible(true);
        List<FeatureVector> buffer = new ArrayList<>();
        bufferField.set(writer, buffer);

        Field lastFlushField = FeatureStoreWriter.class.getDeclaredField("lastFlushMs");
        lastFlushField.setAccessible(true);
        lastFlushField.set(writer, System.currentTimeMillis());

        // Add 5 items — well below flush threshold
        for (int i = 0; i < 5; i++) {
            writer.invoke(sampleVector(), null);
        }
        assertEquals(5, buffer.size());

        // close() should flush remaining
        writer.close();
        assertEquals(0, buffer.size(), "close() should flush remaining buffer");
    }

    // ================================================================
    // Test 9: Batch constants are correct
    // ================================================================

    @Test
    void batchConstantsAreCorrect() {
        assertEquals(100, FeatureStoreWriter.BATCH_SIZE);
        assertEquals(1000L, FeatureStoreWriter.FLUSH_INTERVAL_MS);
        assertEquals(3, FeatureStoreWriter.MAX_RETRIES);
        assertArrayEquals(new long[]{100L, 500L, 2000L}, FeatureStoreWriter.RETRY_DELAYS_MS);
    }
}
