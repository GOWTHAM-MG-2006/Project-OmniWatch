/*
 * OmniWatch — Windowing Layer
 * Component: SlidingWindowAggregator
 * Phase: 4
 * Purpose: Sliding window aggregation operator (5m size, 1m slide) that computes
 *          p50/p95/p99 percentiles (linear interpolation), stddev (population),
 *          and rate (events/sec) per metricName for each keyed entity.
 * Inputs: MetricsEvent stream (keyed by entityId)
 * Outputs: WindowedFeature per metricName -> omniwatch.features.windowed_5m
 */
package com.omniwatch.features.operators;

import com.omniwatch.features.models.MetricsEvent;
import com.omniwatch.features.models.WindowedFeature;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Aggregates metric values within a sliding event-time window and emits one
 * {@link WindowedFeature} per distinct metricName. Computes:
 * <ul>
 *   <li>min, max, avg, count, sum — basic statistics</li>
 *   <li>p50, p95, p99 — percentiles via sorted list + linear interpolation</li>
 *   <li>stddev — population standard deviation (divide by N)</li>
 *   <li>rate — events per second = count / (window duration in seconds)</li>
 * </ul>
 *
 * <p>Usage in FeatureStoreJob:
 * <pre>{@code
 * keyedStream
 *     .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
 *     .process(new SlidingWindowAggregator("5m"));
 * }</pre>
 */
public class SlidingWindowAggregator
        extends ProcessWindowFunction<MetricsEvent, WindowedFeature, String, TimeWindow> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SlidingWindowAggregator.class);

    private final String windowLabel;

    public SlidingWindowAggregator(String windowLabel) {
        this.windowLabel = windowLabel;
    }

    @Override
    public void process(String key, Context context, Iterable<MetricsEvent> elements,
                        Collector<WindowedFeature> out) {
        // Group values by metricName
        Map<String, List<Double>> valuesByMetric = new HashMap<>();
        for (MetricsEvent event : elements) {
            valuesByMetric.computeIfAbsent(event.getMetricName(), k -> new ArrayList<>())
                    .add(event.getValue());
        }

        long windowStart = context.window().getStart();
        long windowEnd = context.window().getEnd();
        double windowDurationSec = (windowEnd - windowStart) / 1000.0;

        for (Map.Entry<String, List<Double>> entry : valuesByMetric.entrySet()) {
            String metricName = entry.getKey();
            List<Double> values = entry.getValue();

            // Sort for percentile and min/max computation
            Collections.sort(values);

            WindowedFeature feature = new WindowedFeature();
            feature.setEntityId(key);
            feature.setWindowStart(windowStart);
            feature.setWindowEnd(windowEnd);
            feature.setWindowSize(windowLabel);
            feature.setMetricName(metricName);

            // Basic statistics
            long count = values.size();
            double sum = 0.0;
            double min = values.get(0);
            double max = values.get((int) count - 1);
            for (double v : values) {
                sum += v;
            }
            double avg = sum / count;

            feature.setMin(min);
            feature.setMax(max);
            feature.setAvg(avg);
            feature.setCount(count);
            feature.setSum(sum);

            // Percentiles via linear interpolation on sorted list
            feature.setP50(percentile(values, 50.0));
            feature.setP95(percentile(values, 95.0));
            feature.setP99(percentile(values, 99.0));

            // Population standard deviation (divide by N, not N-1)
            feature.setStddev(populationStddev(values, avg));

            // Rate = events per second
            feature.setRate(windowDurationSec > 0 ? count / windowDurationSec : 0.0);

            LOG.debug("SlidingWindow [{}] entity={} metric={} count={} p50={} p95={} p99={} stddev={} rate={}",
                    windowLabel, key, metricName, count,
                    feature.getP50(), feature.getP95(), feature.getP99(),
                    feature.getStddev(), feature.getRate());

            out.collect(feature);
        }
    }

    // ---- Package-private static helpers (testable without Flink Context) ----

    /**
     * Computes the p-th percentile (0 &lt; p &lt;= 100) of a <em>sorted</em> list
     * using linear interpolation between the nearest ranks.
     *
     * <p>Algorithm: index = (p/100) * (n - 1). The fractional part interpolates
     * between the two surrounding values. This matches numpy's default
     * {@code linear} interpolation.</p>
     *
     * @param sortedValues already sorted ascending list of values
     * @param p           percentile (1–99 inclusive)
     * @return interpolated percentile value
     * @throws IllegalArgumentException if list is empty or p is out of range
     */
    static double percentile(List<Double> sortedValues, double p) {
        int n = sortedValues.size();
        if (n == 0) {
            throw new IllegalArgumentException("Cannot compute percentile of empty list");
        }
        if (p < 0 || p > 100) {
            throw new IllegalArgumentException("Percentile p must be in [0, 100], got " + p);
        }
        if (n == 1) {
            return sortedValues.get(0);
        }

        double index = (p / 100.0) * (n - 1);
        int lower = (int) Math.floor(index);
        int upper = (int) Math.ceil(index);

        if (lower == upper || upper >= n) {
            return sortedValues.get(lower);
        }

        double fraction = index - lower;
        return sortedValues.get(lower) + fraction * (sortedValues.get(upper) - sortedValues.get(lower));
    }

    /**
     * Computes the population standard deviation (divide by N).
     *
     * <p>Uses the pre-computed mean to avoid a second pass over the data.</p>
     *
     * @param values list of values (not necessarily sorted)
     * @param mean   pre-computed arithmetic mean of {@code values}
     * @return population standard deviation, or 0.0 for a single-element list
     */
    static double populationStddev(List<Double> values, double mean) {
        int n = values.size();
        if (n <= 1) {
            return 0.0;
        }
        double sumSquaredDiff = 0.0;
        for (double v : values) {
            double diff = v - mean;
            sumSquaredDiff += diff * diff;
        }
        return Math.sqrt(sumSquaredDiff / n);
    }
}
