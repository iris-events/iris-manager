package org.iris_events.manager.queue.operations;

import com.rabbitmq.client.GetResponse;

@FunctionalInterface
public interface RouteResolvingFunction {
    RoutingDetails resolve(GetResponse message);
}
