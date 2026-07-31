package com.omniwatch.flink.normalizers;

import com.omniwatch.flink.models.MetricEvent;

/**
 * Normalizes metric values to standard units.
 * Converts bytes to MB, milliseconds to seconds, etc.
 */
public class MetricNormalizer {

    private MetricNormalizer() {
        // Utility class - prevent instantiation
    }

    /**
     * Normalizes a MetricEvent's value based on its unit.
     *
     * <ul>
     *   <li>If unit contains "By" or "byte": converts bytes to megabytes (÷ 1_048_576)</li>
     *   <li>If unit contains "ms" or "milli": converts milliseconds to seconds (÷ 1000)</li>
     *   <li>If unit contains "us" or "micro": converts microseconds to seconds (÷ 1_000_000)</li>
     *   <li>If unit contains "ns" or "nano": converts nanoseconds to seconds (÷ 1_000_000_000)</li>
     *   <li>If unit is "1", "count", or empty: counts remain unchanged</li>
     *   <li>All other units leave value unchanged</li>
     * </ul>
     *
     * @param raw the raw metric event to normalize
     * @return the same event with normalizedValue set
     */
    public static MetricEvent normalize(MetricEvent raw) {
        if (raw == null) {
            return null;
        }

        String unit = raw.getUnit() != null ? raw.getUnit().toLowerCase() : "1";
        double rawValue = raw.getValue();
        double normalizedValue = rawValue;

        if (unit.contains("byte") || unit.contains("by")) {
            // Bytes to megabytes
            normalizedValue = rawValue / 1_048_576.0;
        } else if (unit.contains("ms") || unit.contains("milli")) {
            // Milliseconds to seconds
            normalizedValue = rawValue / 1000.0;
        } else if (unit.contains("us") || unit.contains("micro")) {
            // Microseconds to seconds
            normalizedValue = rawValue / 1_000_000.0;
        } else if (unit.contains("ns") || unit.contains("nano")) {
            // Nanoseconds to seconds
            normalizedValue = rawValue / 1_000_000_000.0;
        } else if ("1".equals(unit) || "count".equals(unit) || unit.isEmpty()) {
            // Count - no conversion needed
            normalizedValue = rawValue;
        }
        // For other units (e.g., "s", "percent", "ratio"), leave as-is

        raw.setNormalizedValue(normalizedValue);
        return raw;
    }
}
