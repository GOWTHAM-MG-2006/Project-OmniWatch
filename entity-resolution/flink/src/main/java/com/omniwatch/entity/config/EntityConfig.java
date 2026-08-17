/*
 * OmniWatch — Entity Resolution Layer
 * Component: EntityConfig
 * Phase: 3
 * Purpose: Loads entity_mappings.yaml (raw id -> provider/region/type/name
 *          extraction patterns) and business_tags.yaml (enrichment rules)
 *          into typed, serializable POJOs used by the pipeline operators.
 * Inputs: src/main/resources/entity_mappings.yaml, business_tags.yaml
 * Outputs: typed ProviderMapping / TagRule structures
 */
package com.omniwatch.entity.config;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Immutable configuration holder for the entity resolution pipeline.
 * Loaded once at job start and shared across parallel operator instances.
 */
public class EntityConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Raw-id extraction pattern. */
    public static class PatternMapping implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String regex;
        private final String type;

        public PatternMapping(String regex, String type) {
            this.regex = regex;
            this.type = type;
        }

        public String getRegex() {
            return regex;
        }

        public String getType() {
            return type;
        }
    }

    /** Provider-scoped mapping block. */
    public static class ProviderMapping implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String provider;
        private final String entityType;
        private final List<PatternMapping> patterns;
        private final boolean regionFromZone;
        private final Map<String, String> zoneToRegion;

        public ProviderMapping(String provider, String entityType,
                               List<PatternMapping> patterns,
                               boolean regionFromZone,
                               Map<String, String> zoneToRegion) {
            this.provider = provider;
            this.entityType = entityType;
            this.patterns = patterns == null ? Collections.emptyList() : patterns;
            this.regionFromZone = regionFromZone;
            this.zoneToRegion = zoneToRegion == null ? Collections.emptyMap() : zoneToRegion;
        }

        public String getProvider() {
            return provider;
        }

        public String getEntityType() {
            return entityType;
        }

        public List<PatternMapping> getPatterns() {
            return patterns;
        }

        public boolean isRegionFromZone() {
            return regionFromZone;
        }

        public Map<String, String> getZoneToRegion() {
            return zoneToRegion;
        }
    }

    /** Business tag enrichment rule. */
    public static class TagRule implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String entityType;
        private final Pattern namePattern;
        private final Map<String, String> tags;

        public TagRule(String entityType, Pattern namePattern, Map<String, String> tags) {
            this.entityType = entityType;
            this.namePattern = namePattern;
            this.tags = tags == null ? Collections.emptyMap() : tags;
        }

        public String getEntityType() {
            return entityType;
        }

        public Pattern getNamePattern() {
            return namePattern;
        }

        public Map<String, String> getTags() {
            return tags;
        }
    }

    private final List<ProviderMapping> mappings;
    private final Map<String, String> defaultTags;
    private final Map<String, Map<String, String>> tagsByType;
    private final List<TagRule> tagRules;

    /** MinIO connection settings — loaded from environment variables with safe defaults. */
    private final String minioEndpoint;
    private final String minioAccessKey;
    private final String minioSecretKey;
    private final String minioBucket;

    private EntityConfig(List<ProviderMapping> mappings,
                         Map<String, String> defaultTags,
                         Map<String, Map<String, String>> tagsByType,
                         List<TagRule> tagRules) {
        this.mappings = mappings == null ? Collections.emptyList() : mappings;
        this.defaultTags = defaultTags == null ? Collections.emptyMap() : defaultTags;
        this.tagsByType = tagsByType == null ? Collections.emptyMap() : tagsByType;
        this.tagRules = tagRules == null ? Collections.emptyList() : tagRules;
        this.minioEndpoint = System.getenv().getOrDefault("MINIO_ENDPOINT", "http://minio:9010");
        this.minioAccessKey = System.getenv().getOrDefault("MINIO_ACCESS_KEY", "minioadmin");
        this.minioSecretKey = System.getenv().getOrDefault("MINIO_SECRET_KEY", "minioadmin");
        this.minioBucket = System.getenv().getOrDefault("MINIO_BUCKET", "omniwatch-telemetry-archive");
    }

    /** Loads both YAML configs from the classpath resources. */
    public static EntityConfig load() {
        try (InputStream mappingsYaml = EntityConfig.class.getResourceAsStream("/entity_mappings.yaml");
             InputStream tagsYaml = EntityConfig.class.getResourceAsStream("/business_tags.yaml")) {
            if (mappingsYaml == null || tagsYaml == null) {
                throw new IllegalStateException("entity_mappings.yaml or business_tags.yaml not found on classpath");
            }
            return load(mappingsYaml, tagsYaml);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to close config streams", e);
        }
    }

    /** Loads config from explicit streams (used by tests with custom YAML). */
    @SuppressWarnings("unchecked")
    public static EntityConfig load(InputStream mappingsYaml, InputStream tagsYaml) {
        Yaml yaml = new Yaml();
        Map<String, Object> mappingsDoc = yaml.load(mappingsYaml);
        Map<String, Object> tagsDoc = yaml.load(tagsYaml);

        List<ProviderMapping> mappings = parseMappings(mappingsDoc);
        Map<String, String> defaultTags = stringMap(nested(tagsDoc, "defaults"));
        Map<String, Map<String, String>> tagsByType = parseTagsByType(tagsDoc);
        List<TagRule> tagRules = parseTagRules(tagsDoc);

        return new EntityConfig(mappings, defaultTags, tagsByType, tagRules);
    }

    @SuppressWarnings("unchecked")
    private static List<ProviderMapping> parseMappings(Map<String, Object> doc) {
        List<ProviderMapping> result = new ArrayList<>();
        Object rawMappings = doc == null ? null : doc.get("mappings");
        if (!(rawMappings instanceof List)) {
            return result;
        }
        for (Object raw : (List<Object>) rawMappings) {
            Map<String, Object> block = (Map<String, Object>) raw;
            String provider = str(block.get("provider"));
            String entityType = str(block.get("entity_type"));
            boolean regionFromZone = Boolean.TRUE.equals(block.get("region_from_zone"));

            List<PatternMapping> patterns = new ArrayList<>();
            Object rawPatterns = block.get("patterns");
            if (rawPatterns instanceof List) {
                for (Object rawPat : (List<Object>) rawPatterns) {
                    Map<String, Object> pat = (Map<String, Object>) rawPat;
                    patterns.add(new PatternMapping(str(pat.get("regex")), str(pat.get("type"))));
                }
            }
            Map<String, String> zoneToRegion = stringMap(nested(block, "zone_to_region"));
            result.add(new ProviderMapping(provider, entityType, patterns, regionFromZone, zoneToRegion));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Map<String, String>> parseTagsByType(Map<String, Object> doc) {
        Map<String, Map<String, String>> result = new LinkedHashMap<>();
        Object raw = doc == null ? null : doc.get("by_entity_type");
        if (!(raw instanceof Map)) {
            return result;
        }
        for (Map.Entry<String, Object> entry : ((Map<String, Object>) raw).entrySet()) {
            result.put(entry.getKey(), stringMap(entry.getValue()));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static List<TagRule> parseTagRules(Map<String, Object> doc) {
        List<TagRule> result = new ArrayList<>();
        Object rawRules = doc == null ? null : doc.get("rules");
        if (!(rawRules instanceof List)) {
            return result;
        }
        for (Object raw : (List<Object>) rawRules) {
            Map<String, Object> rule = (Map<String, Object>) raw;
            String entityType = str(rule.get("entity_type"));
            String nameRegex = str(rule.get("name_regex"));
            Pattern namePattern = nameRegex == null ? null : Pattern.compile(nameRegex);

            Map<String, String> tags = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : rule.entrySet()) {
                String key = entry.getKey();
                if ("entity_type".equals(key) || "name_regex".equals(key)) {
                    continue;
                }
                Object value = entry.getValue();
                if (value instanceof String) {
                    tags.put(key, (String) value);
                }
            }
            result.add(new TagRule(entityType, namePattern, tags));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> nested(Map<String, Object> doc, String key) {
        Object value = doc == null ? null : doc.get(key);
        return value instanceof Map ? (Map<String, Object>) value : Collections.emptyMap();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> stringMap(Object raw) {
        Map<String, String> result = new LinkedHashMap<>();
        if (raw instanceof Map) {
            for (Map.Entry<String, Object> entry : ((Map<String, Object>) raw).entrySet()) {
                Object value = entry.getValue();
                if (value != null) {
                    result.put(entry.getKey(), String.valueOf(value));
                }
            }
        }
        return result;
    }

    private static String str(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    public List<ProviderMapping> getMappings() {
        return mappings;
    }

    public Map<String, String> getDefaultTags() {
        return defaultTags;
    }

    public Map<String, Map<String, String>> getTagsByType() {
        return tagsByType;
    }

    public List<TagRule> getTagRules() {
        return tagRules;
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
}
