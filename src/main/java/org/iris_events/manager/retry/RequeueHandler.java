package org.iris_events.manager.retry;

import static org.iris_events.common.MessagingHeaders.RequeueMessage.X_ORIGINAL_EXCHANGE;
import static org.iris_events.common.MessagingHeaders.RequeueMessage.X_ORIGINAL_QUEUE;
import static org.iris_events.common.MessagingHeaders.RequeueMessage.X_ORIGINAL_ROUTING_KEY;


import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

import org.iris_events.manager.connection.ConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;

import org.iris_events.common.Exchanges;
import org.iris_events.common.Queues;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Consumes message from retry dead letter queue (retry-wait-ended) after retry TTL has expired
 * and publishes the message back to its original exchange.
 */
@ApplicationScoped
public class RequeueHandler {

    private static final Logger log = LoggerFactory.getLogger(RequeueHandler.class);

    private static final String RETRY_WAIT_ENDED_QUEUE_NAME = Queues.RETRY_WAIT_ENDED.getValue();
    private static final String RETRY_EXCHANGE_NAME = Exchanges.RETRY.getValue();

    @Inject
    ConnectionProvider connectionProvider;

    protected Channel channel;

    public void init() {
        try {
            channel = connectionProvider.connect();
            queueBind();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void queueBind() throws IOException {
        log.info("Starting consumer on {} with routing key {}", RETRY_WAIT_ENDED_QUEUE_NAME, RETRY_WAIT_ENDED_QUEUE_NAME);

        channel.basicConsume(RETRY_WAIT_ENDED_QUEUE_NAME, true,
                ((consumerTag, message) -> {
                    // this relays messages from RETRY queues to original queues
                    final var headers = message.getProperties().getHeaders();
                    final var originalExchange = Objects.toString(headers.get(X_ORIGINAL_EXCHANGE));
                    final var originalRoutingKey = Objects.toString(headers.get(X_ORIGINAL_ROUTING_KEY));
                    final var originalQueueHeader = headers.get(X_ORIGINAL_QUEUE);
                    if (originalQueueHeader != null) { // TODO: remove null guard once all services are upgraded to use iris 4.0.3 or higher
                        final var originalQueue = Objects.toString(originalQueueHeader);
                        log.info(
                                "Requeuing message back to original queue. originalQueue={} originalExchange={}, originalRoutingkey={}",
                                originalQueue, originalExchange, originalRoutingKey);

                        channel.basicPublish("", originalQueue, message.getProperties(), message.getBody());
                    } else {
                        log.info("Requeuing message back to original exchange. originalExchange={}, originalRoutingkey={}",
                                originalExchange, originalRoutingKey);
                        channel.basicPublish(originalExchange, originalRoutingKey, message.getProperties(), message.getBody());
                    }
                }),
                consumerTag -> log.warn("Basic consume on {}.{} cancelled. Message for will not be retried",
                        RETRY_EXCHANGE_NAME,
                        RETRY_WAIT_ENDED_QUEUE_NAME),
                (consumerTag, sig) -> log.warn("Consumer for {}.{} shut down. consumerTag: {}, message: {}", RETRY_EXCHANGE_NAME,
                        RETRY_WAIT_ENDED_QUEUE_NAME, consumerTag, sig.getMessage()));
    }
}
