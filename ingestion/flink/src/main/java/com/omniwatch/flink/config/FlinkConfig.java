package com.omniwatch.flink.config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Central configuration for the OmniWatch Flink ingestion job.
 * Reads from command-line arguments and falls back to environment variables,
 * then to hard-coded defaults.
 */
public class FlinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Allowlisted CLI keys that FlinkConfig understands. Any other --key
     * argument is rejected up front so that typos (e.g. --minio.access.key
     * instead of --minio.access-key) fail fast instead of being silently
     * ignored and letting the job hang on unreachable brokers.
     */
    private static final Set<String> KNOWN_KEYS = new HashSet<>(Arrays.asList(
            "kafka.brokers",
            "kafka.group.id",
            "minio.endpoint",
            "minio.access-key",
            "minio.secret-key",
            "minio.bucket",
            "auto.offset.reset"));

    private final String kafkaBrokers;
    private final String kafkaGroupId;
    private final String minioEndpoint;
    private final String minioAccessKey;
    private final String minioSecretKey;
    private final String minioBucket;
    private final String autoOffsetReset;

    private FlinkConfig(String kafkaBrokers, String kafkaGroupId,
                        String minioEndpoint, String minioAccessKey,
                        String minioSecretKey, String minioBucket,
                        String autoOffsetReset) {
        this.kafkaBrokers = kafkaBrokers;
        this.kafkaGroupId = kafkaGroupId;
        this.minioEndpoint = minioEndpoint;
        this.minioAccessKey = minioAccessKey;
        this.minioSecretKey = minioSecretKey;
        this.minioBucket = minioBucket;
        this.autoOffsetReset = autoOffsetReset;
    }

    /**
     * Factory method that builds a FlinkConfig from command-line arguments.
     * Values are resolved from (in priority order):
     * 1. CLI arguments (--key=value)
     * 2. Environment variables
     * 3. Hard-coded defaults
     */
    public static FlinkConfig fromArgs(String[] args) {
        validateCliKeys(args);

        ParameterTool params = ParameterTool.fromArgs(args)
                .mergeWith(ParameterTool.fromSystemProperties());

        String kafkaBrokers = params.get(
                "kafka.brokers",
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"));

        String kafkaGroupId = params.get(
                "kafka.group.id",
                System.getenv().getOrDefault("KAFKA_GROUP_ID", "flink-ingestion"));

        String minioEndpoint = params.get(
                "minio.endpoint",
                System.getenv().getOrDefault("MINIO_ENDPOINT", "http://minio:9010"));

        String minioAccessKey = params.get(
                "minio.access-key",
                System.getenv().getOrDefault("MINIO_ACCESS_KEY", "minioadmin"));

        String minioSecretKey = params.get(
                "minio.secret-key",
                System.getenv().getOrDefault("MINIO_SECRET_KEY", "minioadmin"));

        String minioBucket = params.get(
                "minio.bucket",
                System.getenv().getOrDefault("MINIO_BUCKET", "omniwatch-telemetry-archive"));

        String autoOffsetReset = params.get(
                "auto.offset.reset",
                System.getenv().getOrDefault("AUTO_OFFSET_RESET", "earliest"));

        return new FlinkConfig(
                kafkaBrokers, kafkaGroupId,
                minioEndpoint, minioAccessKey, minioSecretKey,
                minioBucket, autoOffsetReset);
    }

    /**
     * Rejects any --key CLI argument that is not in the allowlist.
     * ParameterTool.fromArgs silently ignores unknown keys, which previously
     * let a typo'd flag reach env.execute() and hang forever retrying
     * unreachable brokers. Both --key=value and --key value forms are checked.
     */
    private static void validateCliKeys(String[] args) {
        if (args == null) {
            return;
        }
        for (String arg : args) {
            if (arg == null || !arg.startsWith("--")) {
                continue;
            }
            String key = arg.substring(2);
            int equalsIndex = key.indexOf('=');
            if (equalsIndex >= 0) {
                key = key.substring(0, equalsIndex);
            }
            if (!KNOWN_KEYS.contains(key)) {
                throw new IllegalArgumentException(
                        "Unknown configuration parameter: --" + key);
            }
        }
    }

    public String getKafkaBrokers() {
        return kafkaBrokers;
    }

    public String getKafkaGroupId() {
        return kafkaGroupId;
    }

    public String getMinioEndpoint() {
        return minioEndpoint;
    }

    public String getMinioAccessKey() {
        return minioAccessKey;
    }

    public String getMinioSecretKey() {
        return minioSecretKey;
    }

    public String getMinioBucket() {
        return minioBucket;
    }

    public String getAutoOffsetReset() {
        return autoOffsetReset;
    }
}
