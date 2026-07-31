/*
 * OmniWatch — Entity Resolution Layer
 * Component: ResourceIdParser
 * Phase: 3
 * Purpose: Stage 1 of the entity resolution pipeline. Matches a raw entity
 *          identifier against the configured extraction patterns and produces
 *          a ParsedResource (provider, region, entity_type, canonical name).
 * Inputs: TelemetryEvent (entityId, entityType hint)
 * Outputs: ParsedResource
 */
package com.omniwatch.entity.operators;

import com.omniwatch.entity.config.EntityConfig;
import com.omniwatch.entity.config.EntityConfig.PatternMapping;
import com.omniwatch.entity.config.EntityConfig.ProviderMapping;
import com.omniwatch.entity.models.ParsedResource;
import com.omniwatch.entity.models.TelemetryEvent;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts structured resource information from raw entity ids.
 * Pattern evaluation is ordered: the first matching pattern across all
 * provider mappings wins.
 */
public class ResourceIdParser implements MapFunction<TelemetryEvent, ParsedResource> {

    private static final long serialVersionUID = 1L;
    private static final String UNKNOWN = "unknown";

    private final EntityConfig config;

    public ResourceIdParser(EntityConfig config) {
        this.config = config;
    }

    @Override
    public ParsedResource map(TelemetryEvent event) {
        String rawId = event.getEntityId() == null ? "" : event.getEntityId();
        return parseRaw(rawId, event.getEntityType(), config);
    }

    /** Static parse entry point, reusable by other operators and tests. */
    public static ParsedResource parseRaw(String rawEntityId, String hintEntityType,
                                          EntityConfig config) {
        if (rawEntityId == null || rawEntityId.isEmpty()) {
            return new ParsedResource("", UNKNOWN, "", hintEntityType == null ? "" : hintEntityType,
                    "", false);
        }
        for (ProviderMapping mapping : config.getMappings()) {
            for (PatternMapping pattern : mapping.getPatterns()) {
                Pattern compiled = Pattern.compile(pattern.getRegex());
                Matcher matcher = compiled.matcher(rawEntityId);
                if (matcher.matches()) {
                    String name = group(matcher, "name", rawEntityId);
                    String region = regionFor(matcher, mapping);
                    String entityType = pattern.getType() != null
                            ? pattern.getType()
                            : (mapping.getEntityType() != null ? mapping.getEntityType() : hintEntityType);
                    return new ParsedResource(rawEntityId, mapping.getProvider(), region,
                            entityType, name, true);
                }
            }
        }
        // No pattern matched: carry the raw id through as an unknown entity.
        return new ParsedResource(rawEntityId, UNKNOWN, "", hintEntityType, rawEntityId, false);
    }

    private static String regionFor(Matcher matcher, ProviderMapping mapping) {
        String zone = group(matcher, "zone", null);
        if (zone == null || zone.isEmpty()) {
            return group(matcher, "region", "");
        }
        if (mapping.isRegionFromZone()) {
            Map<String, String> zoneToRegion = mapping.getZoneToRegion();
            if (zoneToRegion != null && zoneToRegion.containsKey(zone)) {
                return zoneToRegion.get(zone);
            }
        }
        return zone;
    }

    private static String group(Matcher matcher, String name, String fallback) {
        try {
            String value = matcher.group(name);
            return value == null ? (fallback == null ? "" : fallback) : value;
        } catch (IllegalArgumentException e) {
            // Named group absent from this pattern
            return fallback == null ? "" : fallback;
        }
    }
}
