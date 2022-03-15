package id.global.iris.manager.client.model;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public record Queue(
        String name,
        String vhost,
        boolean durable,
        @JsonProperty("auto_delete") boolean autoDelete,
        boolean exclusive,
        Map<String, Object> arguments,
        int consumers,
        int messages,
        @JsonProperty("messages_ready") int messagesReady,
        @JsonProperty("messages_unacknowledged") int messagesUnacknowledged) {
}
