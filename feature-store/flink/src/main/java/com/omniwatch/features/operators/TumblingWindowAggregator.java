/*
 * OmniWatch — Windowing Layer + Feature Store
 * Component: TumblingWindowAggregator
 * Phase: 4
 * Purpose: Aggregates per-metric min/max/avg/count/sum over tumbling windows
 *          (1m, 5m, 15m). Produces one WindowedFeature per metric seen in the
 *          window. Window bounds are injected via the constructor because
 *          AggregateFunction has no window context — FeatureStoreJob instantiates
 *          a new aggregator per window with the correct start/end timestamps.
 * Inputs: MetricsEvent (entityId, metricName, value, timestamp, ...)
 * Outputs: WindowedFeature (one per metric per window)
 */
package com.omniwatch.features.operators;

import com.omniwatch.features.models.MetricsEvent;
import com.omniwatch.features.models.WindowedFeature;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * Tumbling window aggregation operator. Implements {@link AggregateFunction} to
 * compute min, max, sum, count (and derive avg) per metric within a fixed
 * tumbling window.
 *
 * <p><b>Design choice:</b> {@code AggregateFunction} has no window context, so
 * window start/end must be supplied externally. This aggregator accepts
 * {@code windowStart} and {@code windowEnd} in its constructor — the
 * {@code FeatureStoreJob} creates one instance per window (e.g. via
 * {@code TumblingEventTimeWindows.of(...).aggregate(new TumblingWindowAggregator(...))}).
 * For this to work the FeatureStoreJob should use a
 * {@code ProcessWindowFunction}-aware aggregate or pass bounds through a side
 * channel. The simplest correct pattern: expose the ctor with bounds and let the
 * job wire it. If the job uses plain {@code .aggregate()}, windowStart/end are
 * set to 0 and should be overridden by the enclosing ProcessWindowFunction. We
 * document this choice in learnings.md.</p>
 *
 * <p>The accumulator tracks per-metric statistics in a {@link Map} keyed by
 * metricName. {@link #getResult} returns a single {@link WindowedFeature} for
 * the first (or only) metric — in practice the stream is keyed by entityId with
 * a single metricName per window (FeatureStoreJob runs separate windows per
 * metric).</p>
 */
public class TumblingWindowAggregator
        implements AggregateFunction<MetricsEvent, TumblingWindowAggregator.TumblingAccumulator, WindowedFeature> {

    private static final long serialVersionUID = 1L;

    private final String windowLabel;
    private final long windowStart;
    private final long windowEnd;

    /**
     * @param windowLabel human-readable window size ("1m", "5m", "15m")
     * @param windowStart window start timestamp in milliseconds
     * @param windowEnd   window end timestamp in milliseconds
     */
    public TumblingWindowAggregator(String windowLabel, long windowStart, long windowEnd) {
        this.windowLabel = windowLabel;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    /**
     * @param windowLabel human-readable window size ("1m", "5m", "15m")
     */
    public TumblingWindowAggregator(String windowLabel) {
        this(windowLabel, 0L, 0L);
    }

    // ------------------------------------------------------------------ //
    //  Per-metric accumulator                                              //
    // ------------------------------------------------------------------ //

    /**
     * Accumulates raw statistics for a single metric. Values are public for
     * direct access during merge; avg is derived at result time.
     */
    public static class TumblingAccumulator {
        public Map<String, MetricAccum> metrics;

        public TumblingAccumulator() {
            this.metrics = new HashMap<>();
        }

        public TumblingAccumulator(Map<String, MetricAccum> metrics) {
            this.metrics = metrics;
        }
    }

    /**
     * Per-metric running statistics. Mutable POJO with public fields.
     */
    public static class MetricAccum {
        public double min;
        public double max;
        public double sum;
        public long count;

        public MetricAccum() {
            this.min = Double.MAX_VALUE;
            this.max = Double.MIN_VALUE;
            this.sum = 0.0;
            this.count = 0;
        }

        public MetricAccum(double min, double max, double sum, long count) {
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.count = count;
        }
    }

    // ------------------------------------------------------------------ //
    //  AggregateFunction contract                                         //
    // ------------------------------------------------------------------ //

    @Override
    public TumblingAccumulator createAccumulator() {
        return new TumblingAccumulator();
    }

    @Override
    public TumblingAccumulator add(MetricsEvent event, TumblingAccumulator acc) {
        String metricName = event.getMetricName();
        double value = event.getValue();

        MetricAccum m = acc.metrics.get(metricName);
        if (m == null) {
            m = new MetricAccum();
            acc.metrics.put(metricName, m);
        }

        m.min = Math.min(m.min, value);
        m.max = Math.max(m.max, value);
        m.sum += value;
        m.count += 1;

        return acc;
    }

    @Override
    public WindowedFeature getResult(TumblingAccumulator acc) {
        // Return one WindowedFeature for the first (or only) metric in the
        // accumulator. In typical usage the stream contains a single metric
        // per window (FeatureStoreJob keys by metricName or runs separate
        // windowed branches).
        for (Map.Entry<String, MetricAccum> entry : acc.metrics.entrySet()) {
            MetricAccum m = entry.getValue();
            WindowedFeature feature = new WindowedFeature();
            feature.setWindowStart(windowStart);
            feature.setWindowEnd(windowEnd);
            feature.setWindowSize(windowLabel);
            feature.setMetricName(entry.getKey());
            feature.setMin(m.count > 0 ? m.min : 0.0);
            feature.setMax(m.count > 0 ? m.max : 0.0);
            feature.setSum(m.sum);
            feature.setCount(m.count);
            feature.setAvg(m.count > 0 ? m.sum / m.count : 0.0);
            // p50/p95/p99/stddev/rate left at defaults (0.0) — owned by sliding window
            return feature;
        }
        // Empty accumulator (no events) — return a blank feature
        WindowedFeature empty = new WindowedFeature();
        empty.setWindowStart(windowStart);
        empty.setWindowEnd(windowEnd);
        empty.setWindowSize(windowLabel);
        return empty;
    }

    @Override
    public TumblingAccumulator merge(TumblingAccumulator acc1, TumblingAccumulator acc2) {
        for (Map.Entry<String, MetricAccum> entry : acc2.metrics.entrySet()) {
            String metricName = entry.getKey();
            MetricAccum other = entry.getValue();

            MetricAccum merged = acc1.metrics.get(metricName);
            if (merged == null) {
                // Deep-copy the other accumulator so we don't share references
                acc1.metrics.put(metricName,
                        new MetricAccum(other.min, other.max, other.sum, other.count));
            } else {
                merged.min = Math.min(merged.min, other.min);
                merged.max = Math.max(merged.max, other.max);
                merged.sum += other.sum;
                merged.count += other.count;
            }
        }
        return acc1;
    }

    // ------------------------------------------------------------------ //
    //  Accessors (for testing / job wiring)                               //
    // ------------------------------------------------------------------ //

    public String getWindowLabel() {
        return windowLabel;
    }

    public long getWindowStart() {
        return windowStart;
    }

    public long getWindowEnd() {
        return windowEnd;
    }
}
