package com.omniwatch.flink.enrichment;

import com.omniwatch.flink.models.NormalizedEvent;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * RichMapFunction that enriches events with Kubernetes context metadata.
 * Extracts K8s attributes (namespace, pod, container, node) from the event's
 * attributes map when present, and copies them to enrichment attribute keys.
 *
 * @param <T> the event type (must extend NormalizedEvent)
 */
public class K8sContextEnrichment<T extends NormalizedEvent> extends RichMapFunction<T, T> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(K8sContextEnrichment.class);

    private static final String ATTR_K8S_NAMESPACE = "k8s.namespace.name";
    private static final String ATTR_K8S_POD = "k8s.pod.name";
    private static final String ATTR_K8S_CONTAINER = "k8s.container.name";
    private static final String ATTR_K8S_NODE = "k8s.node.name";

    private static final String ATTR_NAMESPACE = "k8s.namespace";
    private static final String ATTR_POD = "k8s.pod";
    private static final String ATTR_CONTAINER = "k8s.container";
    private static final String ATTR_NODE = "k8s.node";

    @Override
    public void open(Configuration parameters) {
        // No resources to initialize
    }

    @Override
    public T map(T event) {
        if (event == null) {
            return null;
        }

        Map<String, String> attrs = event.getAttributes();
        if (attrs == null) {
            return event;
        }

        enrichAttribute(attrs, ATTR_K8S_NAMESPACE, ATTR_NAMESPACE, "k8s.namespace");
        enrichAttribute(attrs, ATTR_K8S_POD, ATTR_POD, "k8s.pod");
        enrichAttribute(attrs, ATTR_K8S_CONTAINER, ATTR_CONTAINER, "k8s.container");
        enrichAttribute(attrs, ATTR_K8S_NODE, ATTR_NODE, "k8s.node");

        return event;
    }

    private void enrichAttribute(Map<String, String> attrs,
                                  String primaryKey, String secondaryKey,
                                  String enrichmentKey) {
        if (attrs.containsKey(enrichmentKey) && attrs.get(enrichmentKey) != null
                && !attrs.get(enrichmentKey).isEmpty()) {
            return;
        }

        String value = attrs.get(primaryKey);
        if (value == null || value.isEmpty()) {
            value = attrs.get(secondaryKey);
        }

        if (value != null && !value.isEmpty()) {
            attrs.put(enrichmentKey, value);
            LOG.debug("Enriched with {} = {}", enrichmentKey, value);
        }
    }

    @Override
    public void close() {
        // No resources to clean up
    }
}
