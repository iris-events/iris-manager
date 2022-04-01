package id.global.iris.manager.retry;

import static id.global.common.headers.amqp.MessagingHeaders.Message.EVENT_TYPE;
import static id.global.common.headers.amqp.MessagingHeaders.RequeueMessage.X_RETRY_COUNT;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;

import id.global.common.headers.amqp.MessagingHeaders;
import id.global.common.iris.Exchanges;
import id.global.common.iris.Queues;
import id.global.iris.manager.InstanceInfoProvider;
import id.global.iris.manager.connection.ConnectionProvider;
import id.global.iris.manager.retry.error.ErrorMessage;
import id.global.iris.messaging.runtime.api.error.ServerError;

/**
 * Consumes messages from general retry queue and publishes them to TTL backoff retry queue.
 */
@ApplicationScoped
public class RetryHandler {

    private static final Logger log = LoggerFactory.getLogger(RetryHandler.class);
    private static final String RETRY_QUEUE_NAME = Queues.RETRY.getValue();
    private static final String RETRY_EXCHANGE_NAME = Exchanges.RETRY.getValue();

    private final ObjectMapper objectMapper;
    private final ConnectionProvider connectionProvider;

    @Inject
    InstanceInfoProvider instanceInfoProvider;

    @Inject
    BackoffQueueProvider backoffQueueProvider;

    @Inject
    RequeueHandler requeueHandler;

    protected Channel channel;

    protected String retryInstanceId;

    @Inject
    public RetryHandler(final ObjectMapper objectMapper, final ConnectionProvider connectionProvider) {
        this.objectMapper = objectMapper;
        this.connectionProvider = connectionProvider;
    }

    public void initialize() {
        log.info("Retry handler starting up...");
        retryInstanceId = instanceInfoProvider.getInstanceName();
        channel = getChanel();
        requeueHandler.init();
        setListener();
    }

    private Channel getChanel() {
        try {
            return connectionProvider.connect();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected void setListener() {
        try {
            channel.basicConsume(RETRY_QUEUE_NAME, true, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                        byte[] body) {

                    log.info("exchange: {}, queue: {}, routing key: {}, deliveryTag: {}", envelope.getExchange(),
                            RETRY_QUEUE_NAME,
                            envelope.getRoutingKey(), envelope.getDeliveryTag());

                    final var message = new AmqpMessage(body, properties, envelope);
                    try {
                        onMessage(message);
                    } catch (Exception e) {
                        log.warn("Error handling retryable message. Message will not be retried.", e);
                    }
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        log.info("consumer started on '{}'", RETRY_QUEUE_NAME);
    }

    public void onMessage(AmqpMessage message) throws IOException {
        final int retryCount = message.retryCount();
        final var maxRetries = message.maxRetries();

        final var maxRetriesReached = retryCount >= maxRetries;
        if (maxRetriesReached) {
            final var originalExchange = message.originalExchange();
            final var originalRoutingKey = message.originalRoutingKey();
            final var errorCode = message.errorCode();
            final var notifyClient = message.notifyClient();
            log.error(String.format(
                    "Could not invoke method handler and max retries (%d) are reached,"
                            + " message is being sent to DLQ. originalExchange=%s, routingKey=%s, errorCode=%s",
                    maxRetries, originalExchange, originalRoutingKey, errorCode));

            if (notifyClient) {
                final var errorMessage = new ErrorMessage(ServerError.SERVER_ERROR.getType().name(),
                        ServerError.SERVER_ERROR.getClientCode(), "Something went wrong");
                sendErrorMessage(errorMessage, message, originalRoutingKey, channel);
            }

            final var deadLetterExchange = message.deadLetterExchange();
            final var deadLetterRoutingKey = message.deadLetterRoutingKey();
            if (deadLetterExchange.isPresent() && deadLetterRoutingKey.isPresent()) {
                channel.basicPublish(deadLetterExchange.get(), deadLetterRoutingKey.get(), message.properties(),
                        message.body());
            }
        } else {
            final var retryQueue = backoffQueueProvider.getQueue(channel, retryCount);
            final var retryQueueName = retryQueue.queueName();
            log.info("Got retryable message: retryCount={}, retryQueue={}", retryCount, retryQueueName);

            final var newMessage = getMessageWithNewHeaders(message, retryCount);
            channel.basicPublish(RETRY_EXCHANGE_NAME, retryQueueName, newMessage.getProperties(), newMessage.getBody());
        }
    }

    private void sendErrorMessage(ErrorMessage message, AmqpMessage consumedMessage, String originalRoutingKey,
            Channel channel) {
        final var headers = new HashMap<>(consumedMessage.properties().getHeaders());
        headers.remove(MessagingHeaders.Message.JWT);
        headers.put(EVENT_TYPE, Exchanges.ERROR.getValue());
        final var basicProperties = consumedMessage.properties().builder()
                .headers(headers)
                .build();
        final var routingKey = originalRoutingKey + ".error";
        try {
            log.info("Sending error message to exchange: {} with routing key: {}", Exchanges.ERROR.getValue(), routingKey);
            channel.basicPublish(Exchanges.ERROR.getValue(), routingKey, basicProperties,
                    objectMapper.writeValueAsBytes(message));
        } catch (IOException e) {
            log.error("Unable to write error message as bytes. Discarding error message. Message: {}", message);
        }
    }

    private Delivery getMessageWithNewHeaders(AmqpMessage message, int retryCount) {
        retryCount += 1;
        final var properties = message.properties();
        final var headers = properties.getHeaders();
        final var newHeaders = new HashMap<>(headers);
        newHeaders.put(X_RETRY_COUNT, retryCount);

        AMQP.BasicProperties basicProperties = new AMQP.BasicProperties().builder().headers(newHeaders)
                .appId(properties.getAppId())
                .correlationId(properties.getCorrelationId())
                .messageId(properties.getMessageId())
                .clusterId(properties.getClusterId())
                .contentEncoding(properties.getContentEncoding())
                .contentType(properties.getContentType())
                .deliveryMode(properties.getDeliveryMode())
                .expiration(properties.getExpiration())
                .priority(properties.getPriority())
                .replyTo(properties.getReplyTo())
                .timestamp(properties.getTimestamp())
                .type(properties.getType())
                .build();

        return new Delivery(message.envelope(), basicProperties, message.body());
    }

}
