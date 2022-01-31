package id.global.iris.manager.retry;

public record RetryQueue(String queueName, long ttl) {
}
