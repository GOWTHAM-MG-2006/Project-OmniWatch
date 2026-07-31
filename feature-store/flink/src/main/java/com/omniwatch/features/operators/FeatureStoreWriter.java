/*
 * OmniWatch — Windowing Layer / Feature Store
 * Component: FeatureStoreWriter
 * Phase: 4
 * Purpose: ClickHouse sink for FeatureVector records — buffers rows, batch
 *          inserts into the feature_vectors table, and handles write failures
 *          with retry + drop semantics to avoid blocking the Flink pipeline.
 * Inputs: FeatureVector records from FeatureVectorBuilder operator
 * Outputs: ClickHouse feature_vectors table (15 columns, MergeTree engine)
 */
package com.omniwatch.features.operators;

import com.omniwatch.features.models.FeatureVector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ClickHouse sink that writes {@link FeatureVector} records in batches.
 *
 * <p>On {@link #open} the sink creates the {@code feature_vectors} table if it
 * does not already exist (MergeTree, partitioned by day, 90-day TTL). Rows are
 * buffered and flushed when the buffer reaches 100 entries or 1 second has
 * elapsed since the last flush. On write failure the batch is retried 3 times
 * with exponential backoff (100 ms, 500 ms, 2 s) then dropped; a static
 * {@code dropped_batches} counter tracks losses. Exceptions are never propagated
 * to the Flink runtime.
 */
public class FeatureStoreWriter extends RichSinkFunction<FeatureVector> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(FeatureStoreWriter.class);

    /** Buffer size threshold for flushing to ClickHouse. */
    static final int BATCH_SIZE = 100;

    /** Maximum milliseconds between flushes even if the batch is not full. */
    static final long FLUSH_INTERVAL_MS = 1000L;

    /** Maximum retry attempts after the initial failure (total tries = MAX_RETRIES + 1). */
    static final int MAX_RETRIES = 3;

    /** Delay before each retry attempt (index 0 = after 1st failure, etc.). */
    static final long[] RETRY_DELAYS_MS = {100L, 500L, 2000L};

    /** DDL executed on open() to ensure the target table exists. */
    static final String DDL = "CREATE TABLE IF NOT EXISTS feature_vectors (\n"
            + "    entity_id String,\n"
            + "    window_start DateTime,\n"
            + "    window_end DateTime,\n"
            + "    window_size String,\n"
            + "    latency_p50 Float64,\n"
            + "    latency_p95 Float64,\n"
            + "    latency_p99 Float64,\n"
            + "    latency_avg Float64,\n"
            + "    latency_min Float64,\n"
            + "    latency_max Float64,\n"
            + "    error_rate Float64,\n"
            + "    request_volume UInt64,\n"
            + "    feature_version UInt32,\n"
            + "    ttl UInt32,\n"
            + "    timestamp DateTime\n"
            + ") ENGINE = MergeTree\n"
            + "PARTITION BY toYYYYMMDD(timestamp)\n"
            + "ORDER BY (entity_id, window_start)\n"
            + "TTL timestamp + INTERVAL 90 DAY";

    /** PreparedStatement parameters — 15 placeholders for the 15 columns. */
    static final String INSERT_SQL = "INSERT INTO feature_vectors "
            + "(entity_id, window_start, window_end, window_size, "
            + "latency_p50, latency_p95, latency_p99, latency_avg, "
            + "latency_min, latency_max, error_rate, request_volume, "
            + "feature_version, ttl, timestamp) "
            + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    /** JDBC driver class name (defensive registration). */
    private static final String CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver";

    static {
        try {
            Class.forName(CLICKHOUSE_DRIVER);
        } catch (ClassNotFoundException e) {
            LOG.warn("ClickHouse JDBC driver class {} not found on classpath", CLICKHOUSE_DRIVER, e);
        }
    }

    // ---- Configuration (set in constructor, immutable) ----

    private final String host;
    private final int port;
    private final String database;

    // ---- Transient runtime state (initialized in open) ----

    private transient Connection connection;
    private transient List<FeatureVector> buffer;
    private transient long lastFlushMs;

    /** Counter of batches dropped after exhausting retries. */
    static final AtomicLong droppedBatches = new AtomicLong(0);

    public FeatureStoreWriter(String host, int port, String database) {
        this.host = host;
        this.port = port;
        this.database = database;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        String url = "jdbc:clickhouse://" + host + ":" + port + "/" + database;
        LOG.info("Connecting to ClickHouse: {}", url);
        connection = DriverManager.getConnection(url);
        buffer = new ArrayList<>(BATCH_SIZE);
        lastFlushMs = System.currentTimeMillis();

        // Ensure the feature_vectors table exists.
        try (PreparedStatement ps = connection.prepareStatement(DDL)) {
            ps.execute();
        }
        LOG.info("FeatureStoreWriter opened — target table feature_vectors ready");
    }

    @Override
    public void invoke(FeatureVector value, Context context) {
        buffer.add(value);
        long now = System.currentTimeMillis();
        if (buffer.size() >= BATCH_SIZE
                || (now - lastFlushMs >= FLUSH_INTERVAL_MS && !buffer.isEmpty())) {
            flush();
        }
    }

    @Override
    public void close() throws Exception {
        flush();
        if (connection != null && !connection.isClosed()) {
            connection.close();
            LOG.info("FeatureStoreWriter connection closed");
        }
    }

    /** Flush the current buffer to ClickHouse with retry + drop semantics. */
    void flush() {
        if (buffer == null || buffer.isEmpty()) {
            return;
        }
        List<FeatureVector> batch = new ArrayList<>(buffer);
        buffer.clear();
        lastFlushMs = System.currentTimeMillis();
        executeBatchInsert(connection, batch);
    }

    // ---- Testable static methods ----

    /**
     * Execute a batch INSERT against the given connection with retry + drop.
     * Package-private for unit testing with a fake connection.
     *
     * @param conn  JDBC connection (may be null — silently ignored)
     * @param batch feature vectors to insert (may be empty — silently ignored)
     */
    static void executeBatchInsert(Connection conn, List<FeatureVector> batch) {
        if (conn == null || batch == null || batch.isEmpty()) {
            return;
        }

        for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
            try {
                PreparedStatement ps = conn.prepareStatement(INSERT_SQL);
                for (FeatureVector fv : batch) {
                    ps.setString(1, fv.getEntityId());
                    ps.setString(2, fv.getWindowStart());
                    ps.setString(3, fv.getWindowEnd());
                    ps.setString(4, fv.getWindowSize());
                    ps.setDouble(5, fv.getLatencyP50());
                    ps.setDouble(6, fv.getLatencyP95());
                    ps.setDouble(7, fv.getLatencyP99());
                    ps.setDouble(8, fv.getLatencyAvg());
                    ps.setDouble(9, fv.getLatencyMin());
                    ps.setDouble(10, fv.getLatencyMax());
                    ps.setDouble(11, fv.getErrorRate());
                    ps.setLong(12, fv.getRequestVolume());
                    ps.setInt(13, fv.getFeatureVersion());
                    ps.setInt(14, fv.getTtl());
                    ps.setString(15, fv.getTimestamp());
                    ps.addBatch();
                }
                ps.executeBatch();
                ps.close();
                return; // success — exit retry loop
            } catch (SQLException e) {
                LOG.error("ClickHouse batch insert failed (attempt {}/{}): {}",
                        attempt + 1, MAX_RETRIES + 1, e.getMessage(), e);
                if (attempt < MAX_RETRIES) {
                    try {
                        Thread.sleep(RETRY_DELAYS_MS[attempt]);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        LOG.warn("Retry sleep interrupted — dropping batch");
                        break;
                    }
                }
            }
        }
        // All retries exhausted — drop the batch.
        droppedBatches.incrementAndGet();
        LOG.error("Dropped batch of {} feature vectors after {} retries",
                batch.size(), MAX_RETRIES);
    }

    /** Return the number of batches dropped due to write failures. */
    public static long getDroppedBatches() {
        return droppedBatches.get();
    }

    /** Reset the dropped-batches counter (for test isolation). */
    static void resetDroppedBatches() {
        droppedBatches.set(0);
    }
}
