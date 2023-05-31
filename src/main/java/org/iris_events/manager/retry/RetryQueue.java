package org.iris_events.manager.retry;

public record RetryQueue(String queueName, long ttl) {
}
