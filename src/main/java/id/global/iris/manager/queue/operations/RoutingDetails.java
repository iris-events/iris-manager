package id.global.iris.manager.queue.operations;

public class RoutingDetails {
    private final String exchange;
    private final String routingKey;
    private final String countHeaderName;

    public RoutingDetails(String exchange, String routingKey, String countHeaderName) {
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.countHeaderName = countHeaderName;
    }

    public String exchange() {
        return exchange;
    }

    public String routingKey() {
        return routingKey;
    }

    public String countHeaderName() {
        return countHeaderName;
    }

    @Override
    public String toString() {
        return "RoutingDetails[" +
                "exchange=" + exchange + ", " +
                "routingKey=" + routingKey + ", " +
                "countHeaderName=" + countHeaderName + ']';
    }

}
