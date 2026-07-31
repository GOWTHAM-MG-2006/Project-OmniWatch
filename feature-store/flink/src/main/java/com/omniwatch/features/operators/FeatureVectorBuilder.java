/*
 * OmniWatch — Windowing Layer
 * Component: FeatureVectorBuilder
 * Phase: 4
 * Purpose: Combines windowed aggregation outputs (from TumblingWindowAggregator
 *          and SlidingWindowAggregator) into 15-column FeatureVector records.
 *          Keyed by entityId, accumulates per-windowSize aggregation state and
 *          emits a FeatureVector on every incoming WindowedFeature using the
 *          latest merged statistics.
 * Inputs: WindowedFeature (union of omniwatch.features.windowed_{1m,5m,15m})
 * Outputs: FeatureVector → Kafka omniwatch.features.vector +
 *          ClickHouse feature_vectors table (via FeatureStoreWriter)
 */
package com.omniwatch.features.operators;

import com.omniwatch.features.models.FeatureVector;
import com.omniwatch.features.models.WindowedFeature;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;

/**
 * Builds 15-column {@link FeatureVector} records from incoming
 * {@link WindowedFeature} aggregations.
 *
 * <p><b>Design choice (documented per plan):</b> emits a FeatureVector on every
 * incoming WindowedFeature using the latest aggregated statistics, rather than
 * buffering until all four target window sizes are populated or using a
 * processing-time timer flush.  This approach is simpler, produces vectors in
 * real-time as windowed aggregations complete, and is straightforward to unit
 * test without timer coordination.</p>
 *
 * <p>State: per-entity (via Flink keyBy entityId) and per-windowSize — a
 * {@code HashMap<String, double[]>} stored in {@link ValueState} with 10
 * accumulator slots:</p>
 * <pre>
 * [0] p50        — latest percentile from sliding window
 * [1] p95        — latest percentile from sliding window
 * [2] p99        — latest percentile from sliding window
 * [3] avg        — weighted average across windows
 * [4] min        — global minimum across windows
 * [5] max        — global maximum across windows
 * [6] totalCount  — sum of request counts
 * [7] windowStart — earliest window start (epoch ms)
 * [8] windowEnd   — latest window end (epoch ms)
 * [9] countForAvg — denominator for weighted average
 * </pre>
 *
 * <p>Latency percentiles (p50/p95/p99) are overwritten on each incoming record
 * since they come from overlapping sliding windows and represent the latest
 * computed value.  Latency avg/min/max are merged: avg via weighted average,
 * min/max via global min/max across all observed windows.  requestVolume is the
 * sum of all incoming counts.  errorRate is set to 0.0 because the
 * WindowedFeature model does not carry an error-specific count — error burst
 * detection is handled upstream by {@code SessionWindowDetector}.</p>
 *
 * <p>Usage (wired in FeatureStoreJob):</p>
 * <pre>{@code
 * unionStream
 *     .keyBy(WindowedFeature::getEntityId)
 *     .process(new FeatureVectorBuilder())
 * }</pre>
 */
public class FeatureVectorBuilder
        extends KeyedProcessFunction<String, WindowedFeature, FeatureVector> {

    private static final long serialVersionUID = 1L;

    /** Number of fields in the aggregation state double[] array. */
    static final int STATE_SIZE = 10;

    /** Indices into the aggregation state array. */
    static final int IDX_P50 = 0;
    static final int IDX_P95 = 1;
    static final int IDX_P99 = 2;
    static final int IDX_AVG = 3;
    static final int IDX_MIN = 4;
    static final int IDX_MAX = 5;
    static final int IDX_TOTAL_COUNT = 6;
    static final int IDX_WINDOW_START = 7;
    static final int IDX_WINDOW_END = 8;
    static final int IDX_COUNT_FOR_AVG = 9;

    /** Shared ISO-8601 formatter (UTC, instant pattern). */
    private static final DateTimeFormatter ISO_FORMATTER =
            DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC);

    /**
     * Per-entity aggregation state: windowSize → double[STATE_SIZE] accumulator.
     * Stored in Flink ValueState for checkpoint/restore compatibility.
     */
    private transient ValueState<HashMap<String, double[]>> aggState;

    /** Per-entity feature version counter (starts at 1, increments per emission). */
    private transient ValueState<Integer> featureVersionState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<HashMap<String, double[]>> aggDesc =
                new ValueStateDescriptor<>(
                        "window-agg",
                        TypeInformation.of(new TypeHint<HashMap<String, double[]>>() {}));
        aggState = getRuntimeContext().getState(aggDesc);

        ValueStateDescriptor<Integer> versionDesc =
                new ValueStateDescriptor<>(
                        "feature-version",
                        TypeInformation.of(Integer.class));
        featureVersionState = getRuntimeContext().getState(versionDesc);
    }

    @Override
    public void processElement(WindowedFeature wf,
                               Context ctx,
                               Collector<FeatureVector> out) throws Exception {
        String windowSize = wf.getWindowSize();
        if (windowSize == null || windowSize.isEmpty()) {
            return;
        }

        // --- 1. Retrieve or initialise per-windowSize aggregation ---
        HashMap<String, double[]> map = aggState.value();
        if (map == null) {
            map = new HashMap<>();
        }

        double[] agg = map.get(windowSize);
        if (agg == null) {
            agg = new double[STATE_SIZE];
            agg[IDX_MIN] = Double.MAX_VALUE;
            agg[IDX_MAX] = Double.NEGATIVE_INFINITY;
        }

        // --- 2. Merge latency percentiles (latest value wins) ---
        agg[IDX_P50] = wf.getP50();
        agg[IDX_P95] = wf.getP95();
        agg[IDX_P99] = wf.getP99();

        // --- 3. Merge latency avg (weighted average across windows) ---
        if (wf.getCount() > 0) {
            double prevSum = agg[IDX_AVG] * agg[IDX_COUNT_FOR_AVG];
            double newSum = prevSum + wf.getAvg() * wf.getCount();
            agg[IDX_COUNT_FOR_AVG] += wf.getCount();
            agg[IDX_AVG] = newSum / agg[IDX_COUNT_FOR_AVG];
        }

        // --- 4. Merge min / max across windows ---
        agg[IDX_MIN] = Math.min(agg[IDX_MIN], wf.getMin());
        agg[IDX_MAX] = Math.max(agg[IDX_MAX], wf.getMax());

        // --- 5. Merge request volume (sum of counts) ---
        agg[IDX_TOTAL_COUNT] += wf.getCount();

        // --- 6. Merge window bounds (earliest start, latest end) ---
        if (agg[IDX_WINDOW_START] == 0 || wf.getWindowStart() < agg[IDX_WINDOW_START]) {
            agg[IDX_WINDOW_START] = wf.getWindowStart();
        }
        if (wf.getWindowEnd() > agg[IDX_WINDOW_END]) {
            agg[IDX_WINDOW_END] = wf.getWindowEnd();
        }

        map.put(windowSize, agg);
        aggState.update(map);

        // --- 7. Increment feature version (starts at 1) ---
        Integer version = featureVersionState.value();
        version = (version == null) ? 1 : version + 1;
        featureVersionState.update(version);

        // --- 8. Build and emit FeatureVector (15 fields) ---
        FeatureVector fv = new FeatureVector();
        fv.setEntityId(wf.getEntityId());
        fv.setWindowStart(toIso8601((long) agg[IDX_WINDOW_START]));
        fv.setWindowEnd(toIso8601((long) agg[IDX_WINDOW_END]));
        fv.setWindowSize(windowSize);
        fv.setLatencyP50(agg[IDX_P50]);
        fv.setLatencyP95(agg[IDX_P95]);
        fv.setLatencyP99(agg[IDX_P99]);
        fv.setLatencyAvg(agg[IDX_AVG]);
        fv.setLatencyMin(agg[IDX_MIN] == Double.MAX_VALUE ? 0.0 : agg[IDX_MIN]);
        fv.setLatencyMax(agg[IDX_MAX] == Double.NEGATIVE_INFINITY ? 0.0 : agg[IDX_MAX]);
        fv.setErrorRate(0.0);  // error count unavailable in WindowedFeature
        fv.setRequestVolume((long) agg[IDX_TOTAL_COUNT]);
        fv.setFeatureVersion(version);
        fv.setTtl(90);
        fv.setTimestamp(ISO_FORMATTER.format(Instant.now()));

        out.collect(fv);
    }

    /** Converts epoch milliseconds to ISO-8601 UTC string. */
    private static String toIso8601(long epochMillis) {
        return ISO_FORMATTER.format(Instant.ofEpochMilli(epochMillis));
    }
}
