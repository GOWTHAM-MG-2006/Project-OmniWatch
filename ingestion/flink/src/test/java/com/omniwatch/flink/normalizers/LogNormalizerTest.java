package com.omniwatch.flink.normalizers;

import com.omniwatch.flink.models.LogEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for LogNormalizer.
 * Verifies severity standardization, whitespace stripping, entity type inference.
 */
class LogNormalizerTest {

    private LogEvent createLog(String serviceName, String severity, String body) {
        LogEvent event = new LogEvent();
        event.setEntityId(serviceName);
        event.setEntityType("API_NODE");
        event.setSourceType("performance");
        event.setServiceName(serviceName);
        event.setSeverity(severity);
        event.setBody(body);
        event.setTimestamp(System.currentTimeMillis());
        return event;
    }

    @Test
    void testStandardizeInfoSeverity() {
        LogEvent event = createLog("svc", "INFO", "test");
        LogNormalizer.normalize(event);
        assertEquals("INFO", event.getSeverity());
    }

    @Test
    void testStandardizeInfoVariants() {
        assertEquals("INFO", LogNormalizer.normalize(createLog("svc", "INFO2", "")).getSeverity());
        assertEquals("INFO", LogNormalizer.normalize(createLog("svc", "INFO3", "")).getSeverity());
        assertEquals("INFO", LogNormalizer.normalize(createLog("svc", "INFO4", "")).getSeverity());
        assertEquals("INFO", LogNormalizer.normalize(createLog("svc", "NOTICE", "")).getSeverity());
    }

    @Test
    void testStandardizeWarnSeverity() {
        LogEvent event = createLog("svc", "WARN2", "test");
        LogNormalizer.normalize(event);
        assertEquals("WARN", event.getSeverity());
    }

    @Test
    void testStandardizeErrorSeverity() {
        assertEquals("ERROR", LogNormalizer.normalize(createLog("svc", "ERROR3", "")).getSeverity());
        assertEquals("ERROR", LogNormalizer.normalize(createLog("svc", "ERROR4", "")).getSeverity());
    }

    @Test
    void testStandardizeFatalSeverity() {
        assertEquals("FATAL", LogNormalizer.normalize(createLog("svc", "FATAL2", "")).getSeverity());
        assertEquals("FATAL", LogNormalizer.normalize(createLog("svc", "CRITICAL", "")).getSeverity());
        assertEquals("FATAL", LogNormalizer.normalize(createLog("svc", "EMERGENCY", "")).getSeverity());
    }

    @Test
    void testNullSeverityDefaultsToInfo() {
        LogEvent event = createLog("svc", null, "test");
        LogNormalizer.normalize(event);
        assertEquals("INFO", event.getSeverity());
    }

    @Test
    void testUnknownSeverityDefaultsToInfo() {
        LogEvent event = createLog("svc", "UNKNOWN_LEVEL", "test");
        LogNormalizer.normalize(event);
        assertEquals("INFO", event.getSeverity());
    }

    @Test
    void testStripExcessWhitespaceFromBody() {
        LogEvent event = createLog("svc", "INFO", "  line1\n\n  line2  \n");
        LogNormalizer.normalize(event);
        assertEquals("line1 line2", event.getBody());
    }

    @Test
    void testNullBodyStaysNull() {
        LogEvent event = createLog("svc", "INFO", null);
        LogNormalizer.normalize(event);
        assertNull(event.getBody());
    }

    @Test
    void testInferEntityTypeFromServiceName() {
        LogEvent event = new LogEvent();
        event.setEntityId("postgresql-database");
        event.setEntityType("API_NODE");
        event.setServiceName("postgresql-database");
        event.setSourceType("performance");
        event.setSeverity("INFO");
        event.setBody("test");
        event.setTimestamp(System.currentTimeMillis());
        LogNormalizer.normalize(event);
        assertEquals("DATABASE_NODE", event.getEntityType());
    }

    @Test
    void testInferEntityTypeForWorker() {
        LogEvent event = createLog("background-worker", "INFO", "test");
        LogNormalizer.normalize(event);
        assertEquals("WORKER_NODE", event.getEntityType());
    }

    @Test
    void testEntityTypeNotOverriddenIfAlreadySet() {
        LogEvent event = createLog("api-gateway", "INFO", "test");
        event.setEntityType("CUSTOM_TYPE");
        LogNormalizer.normalize(event);
        assertEquals("CUSTOM_TYPE", event.getEntityType());
    }

    @Test
    void testNullEventReturnsNull() {
        assertNull(LogNormalizer.normalize(null));
    }
}
