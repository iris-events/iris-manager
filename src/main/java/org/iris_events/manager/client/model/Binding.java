package org.iris_events.manager.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record Binding(
        String source,
        String vhost,
        String destination,
        @JsonProperty("destination_type") String destinationType,
        @JsonProperty("routing_key") String routingKey) {
}
