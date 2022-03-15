package id.global.iris.manager.retry;

import static id.global.common.headers.amqp.MessagingHeaders.Message.EVENT_TYPE;
import static id.global.common.headers.amqp.MessagingHeaders.RequeueMessage.X_RETRY_COUNT;
import static id.global.iris.manager.retry.AmpqMessage.ERR_SERVER_ERROR;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;

import id.global.common.headers.amqp.MessagingHeaders;
import id.global.common.iris.Exchanges;
import id.global.common.iris.Queues;
import id.global.iris.manager.InstanceInfoProvider;
import id.global.iris.manager.retry.error.ErrorMessage;
import io.quarkiverse.rabbitmqclient.RabbitMQClient;
import io.quarkus.runtime.StartupEvent;

/**
 * Consumes messages from general retry queue and publishes them to TTL backoff retry queue.
 */
@ApplicationScoped
public class RetryHandler {

    private static final Logger log = LoggerFactory.getLogger(RetryHandler.class);
    private static final String RETRY_QUEUE_NAME = Queues.RETRY.getValue();
    private static final String RETRY_EXCHANGE_NAME = Exchanges.RETRY.getValue();

    private final ObjectMapper objectMapper;

    @Inject
    RabbitMQClient rabbitMQClient;

    @Inject
    InstanceInfoProvider instanceInfoProvider;

    @Inject
    RetryQueueProvider retryQueueProvider;

    @Inject
    RequeueHandler requeueHandler;

    protected Channel channel;

    protected String retryInstanceId;

    @Inject
    public RetryHandler(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public void onApplicationStart(@Observes StartupEvent event) {
        log.info("Retry handler starting up...");
        retryInstanceId = instanceInfoProvider.getInstanceName();
        channel = createChanel();
        declareExchangeAndQueues();
        retryQueueProvider.declareInitialQueues(channel);
        requeueHandler.init();
    }

    private Channel createChanel() {
        try {
            Connection connection = rabbitMQClient.connect(getConnectionName());
            return connection.createChannel();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected void declareExchangeAndQueues() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-message-ttl", 5000);

        try {
            // declare exchanges and queues
            channel.exchangeDeclare(RETRY_EXCHANGE_NAME, BuiltinExchangeType.DIRECT, true, false, null);
            channel.queueDeclare(RETRY_QUEUE_NAME, true, false, false, args);
            queueBind();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        setListener();
    }

    private void queueBind() throws IOException {
        channel.queueBind(RETRY_QUEUE_NAME, RETRY_EXCHANGE_NAME, RETRY_QUEUE_NAME);
        log.info("binding: '{}' --> '{}' with routing key: '{}'", RETRY_QUEUE_NAME, RETRY_EXCHANGE_NAME,
                RETRY_QUEUE_NAME);
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

                    final var message = new AmpqMessage(body, properties, envelope);
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

    public void onMessage(AmpqMessage message) throws IOException {
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
                final var errorMessage = new ErrorMessage("INTERNAL_SERVER_ERROR", ERR_SERVER_ERROR, "Something went wrong");
                sendErrorMessage(errorMessage, message, originalRoutingKey, channel);
            }

            final var deadLetterExchange = message.deadLetterExchange();
            final var deadLetterRoutingKey = message.deadLetterRoutingKey();
            if (deadLetterExchange.isPresent() && deadLetterRoutingKey.isPresent()) {
                channel.basicPublish(deadLetterExchange.get(), deadLetterRoutingKey.get(), message.properties(),
                        message.body());
            }
        } else {
            final var retryQueue = retryQueueProvider.getQueue(channel, retryCount);
            final var retryQueueName = retryQueue.queueName();
            log.info("Got retryable message: retryCount={}, retryQueue={}", retryCount, retryQueueName);

            final var newMessage = getMessageWithNewHeaders(message, retryCount);
            channel.basicPublish(RETRY_EXCHANGE_NAME, retryQueueName, newMessage.getProperties(), newMessage.getBody());
        }
    }

    private void sendErrorMessage(ErrorMessage message, AmpqMessage consumedMessage, String originalRoutingKey,
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

    private String getConnectionName() {
        final var applicationName = instanceInfoProvider.getApplicationName();
        final var instanceId = instanceInfoProvider.getInstanceName();

        return String.format("%s.%s.%s", RETRY_QUEUE_NAME, applicationName, instanceId);
    }

    private Delivery getMessageWithNewHeaders(AmpqMessage message, int retryCount) {
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
