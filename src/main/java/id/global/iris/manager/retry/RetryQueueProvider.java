package id.global.iris.manager.retry;

import static id.global.iris.manager.Headers.QueueDeclarationHeaders.X_DEAD_LETTER_EXCHANGE;
import static id.global.iris.manager.Headers.QueueDeclarationHeaders.X_DEAD_LETTER_ROUTING_KEY;
import static id.global.iris.manager.Headers.QueueDeclarationHeaders.X_MESSAGE_TTL;
import static id.global.iris.manager.retry.RetryHandler.RETRY_EXCHANGE;
import static id.global.iris.manager.retry.RetryHandler.RETRY_WAIT_ENDED_QUEUE;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import com.rabbitmq.client.Channel;

import id.global.iris.manager.config.Configuration;

@ApplicationScoped
public class RetryQueueProvider {
    private static final String RETRY_QUEUE_TEMPLATE = "retry.retry-queue-%d";

    @Inject
    Configuration config;

    private final ConcurrentHashMap<Long, RetryQueue> ttlRetryQueues = new ConcurrentHashMap<>();

    public void declareInitialQueues(final Channel channel) {
        final var maxRetries = config.retry().maxRetries();
        for (int retryCount = 0; retryCount < maxRetries; retryCount++) {
            final var ttl = getTtl(retryCount);
            ttlRetryQueues.computeIfAbsent(ttl, retryQueue -> declareRetryQueue(channel, ttl));
        }
    }

    public RetryQueue getQueue(final Channel channel, final int retryCount) {
        final var ttl = getTtl(retryCount);
        return ttlRetryQueues.computeIfAbsent(ttl, retryQueue -> declareRetryQueue(channel, ttl));
    }

    private RetryQueue declareRetryQueue(final Channel channel, final Long ttl) {
        final var queueName = String.format(RETRY_QUEUE_TEMPLATE, ttl);
        final var queueDeclarationArgs = getRequeueDeclarationParams(ttl);
        try {
            channel.queueDeclare(queueName, true, false, false, queueDeclarationArgs);
            channel.queueDeclare(RETRY_WAIT_ENDED_QUEUE, true, false, false, null);
            channel.queueBind(queueName, RETRY_EXCHANGE, queueName);

            return new RetryQueue(queueName, ttl);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Long getTtl(int retryCount) {
        final var initialInterval = config.retry().initialInterval();
        final var factor = config.retry().retryFactor();

        return initialInterval + (long) (initialInterval * retryCount * factor);
    }

    private Map<String, Object> getRequeueDeclarationParams(long ttl) {
        return Map.of(
                X_MESSAGE_TTL, ttl,
                X_DEAD_LETTER_ROUTING_KEY, RETRY_WAIT_ENDED_QUEUE,
                X_DEAD_LETTER_EXCHANGE, RETRY_EXCHANGE);
    }
}
