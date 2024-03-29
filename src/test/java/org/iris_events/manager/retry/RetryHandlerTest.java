package org.iris_events.manager.retry;

import static org.iris_events.common.MessagingHeaders.RequeueMessage.X_ORIGINAL_EXCHANGE;
import static org.iris_events.common.MessagingHeaders.RequeueMessage.X_ORIGINAL_QUEUE;
import static org.iris_events.common.MessagingHeaders.RequeueMessage.X_ORIGINAL_ROUTING_KEY;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.iris_events.annotations.ExchangeType;
import org.iris_events.annotations.Message;
import org.iris_events.annotations.MessageHandler;
import org.iris_events.context.IrisContext;
import org.iris_events.runtime.QueueNameProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;

import io.quarkiverse.rabbitmqclient.RabbitMQClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@QuarkusTest
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RetryHandlerTest {

    public static final String RETRIED_MESSAGE_EXCHANGE = "retried-message";
    private Channel channel;

    @Inject
    ObjectMapper objectMapper;

    @Inject
    RabbitMQClient rabbitMQClient;

    @Inject
    Service service;

    @Inject
    QueueNameProvider queueNameProvider;

    @BeforeEach
    void setUp() throws Exception {
        final var connection = rabbitMQClient.connect("JwtAuthIT publisher");
        channel = connection.createChannel(ThreadLocalRandom.current().nextInt(0, 1000));
    }

    @Test
    void retryHandlerShouldRequeueMessage() throws Exception {
        final var messageId = UUID.randomUUID().toString();
        final var message = new RetriedMessage(messageId);
        final var irisContext = new IrisContext();
        irisContext.setExchangeType(ExchangeType.FANOUT);
        irisContext.setName(RETRIED_MESSAGE_EXCHANGE);
        final var queueName = queueNameProvider.getQueueName(irisContext);
        final AMQP.BasicProperties basicProperties = new AMQP.BasicProperties().builder()
                .headers(Map.of(X_ORIGINAL_EXCHANGE, RETRIED_MESSAGE_EXCHANGE,
                        X_ORIGINAL_ROUTING_KEY, "#." + RETRIED_MESSAGE_EXCHANGE,
                        X_ORIGINAL_QUEUE, queueName))
                .build();

        channel.basicPublish("retry", "retry", basicProperties, writeValueAsBytes(message));

        final var retriedMessage = service.getMessage().get(5, TimeUnit.SECONDS);
        assertThat(retriedMessage.id(), is(messageId));
    }

    @ApplicationScoped
    public static class Service {

        private final CompletableFuture<RetriedMessage> retriedMessage = new CompletableFuture<>();

        @Inject
        public Service() {
        }

        @MessageHandler
        public void handle(RetriedMessage message) {
            retriedMessage.complete(message);
        }

        public CompletableFuture<RetriedMessage> getMessage() {
            return retriedMessage;
        }

    }

    @Message(name = RETRIED_MESSAGE_EXCHANGE)
    record RetriedMessage(String id) {
    }

    private byte[] writeValueAsBytes(Object value) throws RuntimeException {
        try {
            return objectMapper.writeValueAsBytes(value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Could not serialize to json", e);
        }
    }
}
