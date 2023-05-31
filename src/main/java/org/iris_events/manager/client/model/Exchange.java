package org.iris_events.manager.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record Exchange(
        String name,
        String vhost,
        String type,
        boolean durable,
        @JsonProperty("auto_delete") boolean autoDelete,
        boolean internal) {
}
