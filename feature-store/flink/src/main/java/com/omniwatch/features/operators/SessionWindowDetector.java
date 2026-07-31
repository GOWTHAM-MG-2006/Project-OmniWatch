/*
 * OmniWatch — Windowing Layer + Feature Store
 * Component: SessionWindowDetector
 * Phase: 4
 * Purpose: Session-windowed error burst detector. Keyed by entityId, uses
 *          EventTimeSessionWindows with a 30s gap to group events. Counts
 *          error events per session and flags bursts when the count exceeds
 *          the configured threshold.
 * Inputs: MetricsEvent stream (keyed by entityId, session-windowed)
 * Outputs: SessionFeature → omniwatch.features.session (Kafka)
 */
package com.omniwatch.features.operators;

import com.omniwatch.features.models.MetricsEvent;
import com.omniwatch.features.models.SessionFeature;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Detects error bursts within session windows.
 *
 * <p>Configured via {@link org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows}
 * with a 30-second gap. Each session window is processed independently: the
 * operator counts events where {@link MetricsEvent#isError()} is {@code true}
 * and sets {@link SessionFeature#isBurstFlag()} to {@code true} when the
 * error count exceeds the constructor-supplied threshold.</p>
 *
 * <p>Usage (wired in FeatureStoreJob):</p>
 * <pre>{@code
 * keyedStream
 *     .window(EventTimeSessionWindows.withGap(Time.seconds(30)))
 *     .process(new SessionWindowDetector(3));
 * }</pre>
 */
public class SessionWindowDetector
        extends ProcessWindowFunction<MetricsEvent, SessionFeature, String, TimeWindow> {

    private static final long serialVersionUID = 1L;

    private final int burstThreshold;

    /**
     * @param burstThreshold maximum error count before a session is flagged
     *                       as a burst (exclusive: burst if errorCount > threshold)
     */
    public SessionWindowDetector(int burstThreshold) {
        this.burstThreshold = burstThreshold;
    }

    @Override
    public void process(String key,
                        Context context,
                        Iterable<MetricsEvent> elements,
                        Collector<SessionFeature> out) {

        int errorCount = 0;
        for (MetricsEvent event : elements) {
            if (event.isError()) {
                errorCount++;
            }
        }

        SessionFeature feature = evaluate(
                key,
                context.window().getStart(),
                context.window().getEnd(),
                errorCount,
                burstThreshold);

        out.collect(feature);
    }

    /**
     * Pure evaluation logic extracted for deterministic unit testing.
     *
     * @param entityId       the keyed entity identifier
     * @param start          session window start timestamp (ms)
     * @param end            session window end timestamp (ms)
     * @param errorCount     number of error events observed in this session
     * @param burstThreshold maximum error count before flagging as burst
     * @return SessionFeature with burstFlag set when errorCount > burstThreshold
     */
    static SessionFeature evaluate(String entityId, long start, long end,
                                   int errorCount, int burstThreshold) {
        return new SessionFeature(
                entityId,
                start,
                end,
                errorCount,
                errorCount > burstThreshold);
    }
}
