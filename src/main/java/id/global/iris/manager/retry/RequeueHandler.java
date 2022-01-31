package id.global.iris.manager.retry;

import static id.global.iris.manager.Headers.RequeueHeaders.X_ORIGINAL_EXCHANGE;
import static id.global.iris.manager.Headers.RequeueHeaders.X_ORIGINAL_ROUTING_KEY;
import static id.global.iris.manager.retry.RetryHandler.RETRY_EXCHANGE;
import static id.global.iris.manager.retry.RetryHandler.RETRY_WAIT_ENDED_QUEUE;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.UUID;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import id.global.iris.manager.InstanceInfoProvider;
import io.quarkiverse.rabbitmqclient.RabbitMQClient;

/**
 * Consumes message from retry dead letter queue (retry-wait-ended) after retry TTL has expired
 * and publishes the message back to its original exchange.
 */
@ApplicationScoped
public class RequeueHandler {

    private static final Logger log = LoggerFactory.getLogger(RequeueHandler.class);

    @Inject
    RabbitMQClient rabbitMQClient;

    @Inject
    InstanceInfoProvider instanceInfoProvider;

    protected Channel channel;
    protected String retryInstanceId;

    public void init() {
        retryInstanceId = instanceInfoProvider.getInstanceName();
        final var channelId = UUID.randomUUID().toString();
        try {
            channel = createChanel(channelId);
            queueBind();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Channel createChanel(final String channelId) throws IOException {
        Connection connection = rabbitMQClient.connect(channelId);
        return connection.createChannel();
    }

    private void queueBind() throws IOException {
        channel.queueDeclare(RETRY_WAIT_ENDED_QUEUE, true, false, false, null);
        channel.queueBind(RETRY_WAIT_ENDED_QUEUE, RETRY_EXCHANGE, RETRY_WAIT_ENDED_QUEUE);
        log.info("Starting consumer on {} with routing key {}", RETRY_WAIT_ENDED_QUEUE, RETRY_WAIT_ENDED_QUEUE);

        channel.basicConsume(RETRY_WAIT_ENDED_QUEUE, true,
                ((consumerTag, message) -> {
                    // this relays messages from RETRY queues to original queues
                    final var headers = message.getProperties().getHeaders();
                    final var originalExchange = Objects.toString(headers.get(X_ORIGINAL_EXCHANGE));
                    final var originalRoutingKey = Objects.toString(headers.get(X_ORIGINAL_ROUTING_KEY));
                    log.info("Requeuing message back to original exchange. originalExchange={}, originalRoutingkey={}",
                            originalExchange, originalRoutingKey);

                    channel.basicPublish(originalExchange, originalRoutingKey, message.getProperties(), message.getBody());
                }), consumerTag -> log.warn("Basic consume on {}.{} cancelled. Message for will not be retried", RETRY_EXCHANGE,
                        RETRY_WAIT_ENDED_QUEUE),
                (consumerTag, sig) -> log.warn("Consumer for {}.{} shut down.", RETRY_EXCHANGE, RETRY_WAIT_ENDED_QUEUE));
    }
}
