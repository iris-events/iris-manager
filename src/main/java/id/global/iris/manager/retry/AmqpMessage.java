package id.global.iris.manager.retry;

import static id.global.common.iris.constants.MessagingHeaders.Message.SESSION_ID;
import static id.global.common.iris.constants.MessagingHeaders.Message.USER_ID;
import static id.global.common.iris.constants.MessagingHeaders.QueueDeclaration.X_DEAD_LETTER_EXCHANGE;
import static id.global.common.iris.constants.MessagingHeaders.QueueDeclaration.X_DEAD_LETTER_ROUTING_KEY;
import static id.global.common.iris.constants.MessagingHeaders.RequeueMessage.X_ERROR_CODE;
import static id.global.common.iris.constants.MessagingHeaders.RequeueMessage.X_MAX_RETRIES;
import static id.global.common.iris.constants.MessagingHeaders.RequeueMessage.X_NOTIFY_CLIENT;
import static id.global.common.iris.constants.MessagingHeaders.RequeueMessage.X_ORIGINAL_EXCHANGE;
import static id.global.common.iris.constants.MessagingHeaders.RequeueMessage.X_ORIGINAL_ROUTING_KEY;
import static id.global.common.iris.constants.MessagingHeaders.RequeueMessage.X_RETRY_COUNT;
import static id.global.iris.manager.retry.RetryHandler.SERVER_ERROR_CLIENT_CODE;

import java.util.Optional;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

public record AmqpMessage(byte[] body, AMQP.BasicProperties properties, Envelope envelope) {

    public String userId() {
        return getStringHeader(properties, USER_ID);
    }

    public String sessionId() {
        return getStringHeader(properties, SESSION_ID);
    }

    public Optional<String> deadLetterExchange() {
        return Optional.ofNullable(getStringHeader(properties, X_DEAD_LETTER_EXCHANGE));
    }

    public Optional<String> deadLetterRoutingKey() {
        return Optional.ofNullable(getStringHeader(properties, X_DEAD_LETTER_ROUTING_KEY));
    }

    public String originalExchange() {
        return getStringHeader(properties, X_ORIGINAL_EXCHANGE);
    }

    public String originalRoutingKey() {
        return getStringHeader(properties, X_ORIGINAL_ROUTING_KEY);
    }

    public String errorCode() {
        return Optional.ofNullable(getStringHeader(properties, X_ERROR_CODE))
                .orElse(SERVER_ERROR_CLIENT_CODE);
    }

    public boolean notifyClient() {
        return Optional.ofNullable(getStringHeader(properties, X_NOTIFY_CLIENT))
                .map(Boolean::valueOf)
                .orElse(false);
    }

    public int maxRetries() {
        return Optional.ofNullable(getStringHeader(properties, X_MAX_RETRIES))
                .map(Integer::valueOf)
                .orElse(1);
    }

    public Integer retryCount() {
        return Optional.ofNullable(getStringHeader(properties, X_RETRY_COUNT)).map(Integer::valueOf).orElse(0);
    }

    private String getStringHeader(AMQP.BasicProperties props, String name) {
        var r = props.getHeaders().get(name);
        if (r != null) {
            return r.toString();
        }
        return null;
    }
}
