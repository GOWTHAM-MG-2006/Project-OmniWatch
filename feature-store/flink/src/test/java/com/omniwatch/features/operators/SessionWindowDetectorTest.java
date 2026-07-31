/*
 * OmniWatch — Windowing Layer + Feature Store
 * Component: SessionWindowDetectorTest
 * Phase: 4
 * Purpose: Unit tests for the session window error burst detector.
 *          Validates evaluate() logic directly for deterministic, fast,
 *          Flink-infrastructure-free assertions.
 * Inputs: MetricsEvent model, SessionFeature model
 * Outputs: SessionFeature assertion checks
 */
package com.omniwatch.features.operators;

import com.omniwatch.features.models.SessionFeature;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SessionWindowDetectorTest {

    private static final int BURST_THRESHOLD = 3;

    /**
     * 4 errors within a 20s session → burstFlag = true (4 > 3).
     * This validates the core burst detection logic.
     */
    @Test
    void burstFlagTrueWhenErrorCountExceedsThreshold() {
        SessionFeature result = SessionWindowDetector.evaluate(
                "entity-web-1", 1000L, 21000L, 4, BURST_THRESHOLD);

        assertEquals("entity-web-1", result.getEntityId());
        assertEquals(1000L, result.getSessionStart());
        assertEquals(21000L, result.getSessionEnd());
        assertEquals(4, result.getErrorCount());
        assertTrue(result.isBurstFlag(), "4 errors > threshold 3 must be flagged as burst");
    }

    /**
     * 2 errors spread over two separate sessions (each ≤3) → burstFlag = false.
     * Session 1: 1 error at t=0..25s. Session 2: 1 error at t=60..80s.
     * Both individually have errorCount=1 which is ≤3.
     */
    @Test
    void burstFlagFalseWhenErrorCountBelowThreshold() {
        SessionFeature session1 = SessionWindowDetector.evaluate(
                "entity-web-1", 0L, 25000L, 1, BURST_THRESHOLD);
        SessionFeature session2 = SessionWindowDetector.evaluate(
                "entity-web-1", 60000L, 80000L, 1, BURST_THRESHOLD);

        assertFalse(session1.isBurstFlag(), "1 error ≤ threshold 3 must not be flagged");
        assertFalse(session2.isBurstFlag(), "1 error ≤ threshold 3 must not be flagged");

        assertEquals("entity-web-1", session1.getEntityId());
        assertEquals("entity-web-1", session2.getEntityId());
        assertEquals(0L, session1.getSessionStart());
        assertEquals(60000L, session2.getSessionStart());
    }

    /**
     * Exactly at threshold (3 errors) → burstFlag = false (strictly greater).
     */
    @Test
    void burstFlagFalseWhenErrorCountEqualsThreshold() {
        SessionFeature result = SessionWindowDetector.evaluate(
                "entity-db-1", 5000L, 35000L, 3, BURST_THRESHOLD);

        assertFalse(result.isBurstFlag(), "3 errors == threshold 3 must not be flagged (strict >)");
        assertEquals(3, result.getErrorCount());
    }

    /**
     * Zero errors → burstFlag = false, errorCount = 0.
     */
    @Test
    void burstFlagFalseWhenNoErrors() {
        SessionFeature result = SessionWindowDetector.evaluate(
                "entity-api-1", 1000L, 31000L, 0, BURST_THRESHOLD);

        assertFalse(result.isBurstFlag());
        assertEquals(0, result.getErrorCount());
    }

    /**
     * Large burst (10 errors) → burstFlag = true.
     */
    @Test
    void burstFlagTrueForLargeErrorCount() {
        SessionFeature result = SessionWindowDetector.evaluate(
                "entity-svc-1", 0L, 30000L, 10, BURST_THRESHOLD);

        assertTrue(result.isBurstFlag(), "10 errors > threshold 3 must be flagged");
        assertEquals(10, result.getErrorCount());
    }

    /**
     * Custom threshold: threshold=5, 4 errors → burstFlag = false.
     */
    @Test
    void respectsCustomThreshold() {
        SessionFeature result = SessionWindowDetector.evaluate(
                "entity-svc-2", 0L, 30000L, 4, 5);

        assertFalse(result.isBurstFlag(), "4 errors ≤ custom threshold 5 must not be flagged");

        SessionFeature result2 = SessionWindowDetector.evaluate(
                "entity-svc-2", 0L, 30000L, 6, 5);
        assertTrue(result2.isBurstFlag(), "6 errors > custom threshold 5 must be flagged");
    }

    /**
     * Session boundary fields are preserved exactly.
     */
    @Test
    void preservesSessionBoundaries() {
        long start = 1700000000000L;
        long end = 1700000030000L;

        SessionFeature result = SessionWindowDetector.evaluate(
                "entity-x", start, end, 2, BURST_THRESHOLD);

        assertEquals(start, result.getSessionStart());
        assertEquals(end, result.getSessionEnd());
        assertEquals("entity-x", result.getEntityId());
    }
}
