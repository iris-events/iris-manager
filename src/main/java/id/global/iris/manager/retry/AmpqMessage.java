package id.global.iris.manager.retry;

import static id.global.iris.manager.Headers.RequeueHeaders.X_ERROR_CODE;
import static id.global.iris.manager.Headers.RequeueHeaders.X_MAX_RETRIES;
import static id.global.iris.manager.Headers.RequeueHeaders.X_NOTIFY_CLIENT;
import static id.global.iris.manager.Headers.RequeueHeaders.X_ORIGINAL_EXCHANGE;
import static id.global.iris.manager.Headers.RequeueHeaders.X_ORIGINAL_ROUTING_KEY;
import static id.global.iris.manager.Headers.RequeueHeaders.X_RETRY_COUNT;

import java.util.Optional;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

public record AmpqMessage(byte[] body, AMQP.BasicProperties properties, Envelope envelope) {

    public static final String ERR_SERVER_ERROR = "ERR_SERVER_ERROR";

    public String userId() {
        return getStringHeader(properties, "userId");
    }

    public String correlationId() {
        return properties.getCorrelationId();
    }

    public String clientTraceId() {
        return getStringHeader(properties, "clientTraceId");
    }

    public String sessionId() {
        return getStringHeader(properties, "sessionId");
    }

    public String currentServiceId() {
        return getStringHeader(properties, "currentServiceId");
    }

    public String routerId() {
        return getStringHeader(properties, "router");
    }

    public String originalExchange() {
        return getStringHeader(properties, X_ORIGINAL_EXCHANGE);
    }

    public String originalRoutingKey() {
        return getStringHeader(properties, X_ORIGINAL_ROUTING_KEY);
    }

    public String errorCode() {
        return Optional.ofNullable(getStringHeader(properties, X_ERROR_CODE))
                .orElse(ERR_SERVER_ERROR);
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
