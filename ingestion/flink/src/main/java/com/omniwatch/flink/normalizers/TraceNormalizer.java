package com.omniwatch.flink.normalizers;

import com.omniwatch.flink.models.TraceEvent;

/**
 * Normalizes trace span events to a canonical format.
 * Ensures traceId is 32-char hex, spanId is 16-char hex,
 * and status is mapped to canonical values.
 */
public class TraceNormalizer {

    private TraceNormalizer() {
        // Utility class - prevent instantiation
    }

    /**
     * Normalizes a TraceEvent to canonical form.
     * <ul>
     *   <li>Pads or truncates traceId to 32 hex characters</li>
     *   <li>Pads or truncates spanId to 16 hex characters</li>
     *   <li>Maps status codes: null/"0" → "UNSET", "1" → "OK", "2" → "ERROR"</li>
     * </ul>
     *
     * @param raw the raw trace event
     * @return the normalized trace event
     */
    public static TraceEvent normalize(TraceEvent raw) {
        if (raw == null) {
            return null;
        }

        // Normalize traceId to 32-char hex
        if (raw.getTraceId() != null) {
            raw.setTraceId(normalizeHexId(raw.getTraceId(), 32));
        }

        // Normalize spanId to 16-char hex
        if (raw.getSpanId() != null) {
            raw.setSpanId(normalizeHexId(raw.getSpanId(), 16));
        }

        // Normalize parentSpanId: if present and non-empty, ensure 16-char hex
        if (raw.getParentSpanId() != null && !raw.getParentSpanId().isEmpty()) {
            raw.setParentSpanId(normalizeHexId(raw.getParentSpanId(), 16));
        }

        // Normalize status
        String status = raw.getStatus();
        if (status == null || status.isEmpty() || "0".equals(status)) {
            raw.setStatus("UNSET");
        } else if ("1".equals(status)) {
            raw.setStatus("OK");
        } else if ("2".equals(status)) {
            raw.setStatus("ERROR");
        } else {
            // Already a string like "UNSET", "OK", "ERROR"
            String upper = status.toUpperCase();
            if ("UNSET".equals(upper) || "OK".equals(upper) || "ERROR".equals(upper)) {
                raw.setStatus(upper);
            } else {
                raw.setStatus("UNSET");
            }
        }

        return raw;
    }

    /**
     * Pads or truncates a hex string to the specified length.
     * If the string is shorter, left-pads with zeros.
     * If the string is longer, truncates from the left.
     *
     * @param hex     the raw hex string
     * @param length  the target length
     * @return the normalized hex string
     */
    private static String normalizeHexId(String hex, int length) {
        // Strip non-hex characters (e.g., "0x" prefix)
        String cleaned = hex.replaceAll("[^0-9a-fA-F]", "");

        if (cleaned.length() > length) {
            // Truncate from the left (take the last `length` chars)
            return cleaned.substring(cleaned.length() - length);
        } else if (cleaned.length() < length) {
            // Left-pad with zeros
            StringBuilder sb = new StringBuilder(length);
            for (int i = 0; i < length - cleaned.length(); i++) {
                sb.append('0');
            }
            sb.append(cleaned);
            return sb.toString();
        }
        return cleaned.toLowerCase();
    }
}
