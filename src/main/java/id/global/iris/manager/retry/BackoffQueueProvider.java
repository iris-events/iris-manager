package id.global.iris.manager.retry;

import static id.global.iris.common.constants.MessagingHeaders.QueueDeclaration.X_DEAD_LETTER_EXCHANGE;
import static id.global.iris.common.constants.MessagingHeaders.QueueDeclaration.X_DEAD_LETTER_ROUTING_KEY;
import static id.global.iris.common.constants.MessagingHeaders.QueueDeclaration.X_MESSAGE_TTL;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.rabbitmq.client.Channel;

import id.global.iris.common.constants.Exchanges;
import id.global.iris.common.constants.Queues;
import id.global.iris.manager.config.Configuration;
import id.global.iris.manager.infrastructure.InfrastructureDeclarator;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class BackoffQueueProvider {
    private static final String RETRY_QUEUE_TEMPLATE = Queues.RETRY_WAIT_TTL_PREFIX.getValue() + "%d";

    @Inject
    Configuration config;

    @Inject
    InfrastructureDeclarator infrastructureDeclarator;

    private final ConcurrentHashMap<Long, RetryQueue> ttlRetryQueues = new ConcurrentHashMap<>();

    public void declareBackoffQueues(final Channel channel) {
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
        final var args = getRequeueDeclarationParams(ttl);
        try {
            final var details = new InfrastructureDeclarator.QueueDeclarationDetails(queueName, true, false, false, args);
            infrastructureDeclarator.declareQueueWithRecreateOnConflict(channel, details);
            channel.queueBind(queueName, Exchanges.RETRY.getValue(), queueName);

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
                X_DEAD_LETTER_ROUTING_KEY, Queues.RETRY_WAIT_ENDED.getValue(),
                X_DEAD_LETTER_EXCHANGE, Exchanges.RETRY.getValue());
    }
}
