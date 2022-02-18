package id.global.iris.manager.queue.operations;

public record RoutingDetails(String exchange, String routingKey, String countHeaderName) {
}
