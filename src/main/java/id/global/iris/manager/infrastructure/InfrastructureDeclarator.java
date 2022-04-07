package id.global.iris.manager.infrastructure;

import static id.global.common.constants.iris.MessagingHeaders.QueueDeclaration.X_MESSAGE_TTL;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;

import id.global.common.constants.iris.Exchanges;
import id.global.common.constants.iris.Queues;
import id.global.iris.manager.connection.ConnectionProvider;
import id.global.iris.manager.retry.BackoffQueueProvider;

@ApplicationScoped
public class InfrastructureDeclarator {

    private static final Logger log = LoggerFactory.getLogger(InfrastructureDeclarator.class);
    private static final int RETRY_QUEUE_TTL = 5000;

    private static final String ERROR_EXCHANGE = Exchanges.ERROR.getValue();
    private static final String RETRY_EXCHANGE = Exchanges.RETRY.getValue();
    private static final String DEAD_LETTER_EXCHANGE = Exchanges.DEAD_LETTER.getValue();
    private static final String FRONTEND_EXCHANGE = Exchanges.FRONTEND.getValue();
    private static final String SESSION_EXCHANGE = Exchanges.SESSION.getValue();
    private static final String USER_EXCHANGE = Exchanges.USER.getValue();
    private static final String BROADCAST_EXCHANGE = Exchanges.BROADCAST.getValue();
    private static final String SUBSCRIPTION_EXCHANGE = Exchanges.SUBSCRIPTION.getValue();

    private static final String ERROR_QUEUE = Queues.ERROR.getValue();
    private static final String DEAD_LETTER_QUEUE = Queues.DEAD_LETTER.getValue();
    private static final String RETRY_QUEUE = Queues.RETRY.getValue();
    private static final String RETRY_WAIT_ENDED_QUEUE = Queues.RETRY_WAIT_ENDED.getValue();
    private static final String SUBSCRIPTION_QUEUE = Queues.SUBSCRIPTION.getValue();

    private static final Map<Exchanges, ExchangeDeclarationDetails> exchanges = Map.of(
            Exchanges.ERROR, new ExchangeDeclarationDetails(ERROR_EXCHANGE, BuiltinExchangeType.TOPIC, true),
            Exchanges.RETRY, new ExchangeDeclarationDetails(RETRY_EXCHANGE, BuiltinExchangeType.DIRECT, true),
            Exchanges.DEAD_LETTER, new ExchangeDeclarationDetails(DEAD_LETTER_EXCHANGE, BuiltinExchangeType.TOPIC, true),
            Exchanges.FRONTEND, new ExchangeDeclarationDetails(FRONTEND_EXCHANGE, BuiltinExchangeType.TOPIC, true),
            Exchanges.SESSION, new ExchangeDeclarationDetails(SESSION_EXCHANGE, BuiltinExchangeType.TOPIC, true),
            Exchanges.USER, new ExchangeDeclarationDetails(USER_EXCHANGE, BuiltinExchangeType.TOPIC, true),
            Exchanges.BROADCAST, new ExchangeDeclarationDetails(BROADCAST_EXCHANGE, BuiltinExchangeType.TOPIC, true),
            Exchanges.SUBSCRIPTION, new ExchangeDeclarationDetails(SUBSCRIPTION_EXCHANGE, BuiltinExchangeType.TOPIC, true));

    private static final Map<Queues, QueueDeclarationDetails> queues = Map.of(
            Queues.ERROR, new QueueDeclarationDetails(ERROR_QUEUE, true, false, false, null),
            Queues.RETRY, new QueueDeclarationDetails(RETRY_QUEUE, true, false, false, Map.of(X_MESSAGE_TTL, RETRY_QUEUE_TTL)),
            Queues.RETRY_WAIT_ENDED, new QueueDeclarationDetails(RETRY_WAIT_ENDED_QUEUE, true, false, false, null),
            Queues.DEAD_LETTER, new QueueDeclarationDetails(DEAD_LETTER_QUEUE, true, false, false, null),
            Queues.SUBSCRIPTION, new QueueDeclarationDetails(SUBSCRIPTION_QUEUE, true, false, false, null));

    @Inject
    ConnectionProvider connectionProvider;

    @Inject
    BackoffQueueProvider backoffQueueProvider;

    public void declareBackboneInfrastructure() {
        log.info("Initializing backbone Iris infrastructure (exchanges, queues).");
        try (Channel channel = connectionProvider.connect()) {
            declareExchanges(channel);
            declareQueues(channel);
            bindQueues(channel);
            backoffQueueProvider.declareBackoffQueues(channel);
        } catch (IOException ioException) {
            throw new UncheckedIOException(ioException);
        } catch (TimeoutException timeoutException) {
            throw new RuntimeException(timeoutException);
        }
    }

    public void declareQueueWithRecreateOnConflict(final Channel channel, final QueueDeclarationDetails details)
            throws IOException {
        final var queueName = details.queueName;
        try {
            declareQueue(details);
        } catch (IOException e) {
            long msgCount = channel.messageCount(queueName);
            if (msgCount <= 0) {
                log.warn("Queue declaration parameters changed. Trying to re-declare queue. Details: "
                        + e.getCause().getMessage());
                channel.queueDelete(queueName, false, true);
                declareQueue(details);
            } else {
                log.error("The new settings of queue was not set, because was not empty! queue={}", queueName, e);
            }
        }
    }

    private void declareExchanges(final Channel channel) throws IOException {
        for (var exchangeDeclarationDetails : exchanges.values()) {
            declareExchange(channel, exchangeDeclarationDetails);
        }
    }

    private void declareQueues(final Channel channel) throws IOException {
        for (var queueDeclarationDetails : queues.values()) {
            declareQueueWithRecreateOnConflict(channel, queueDeclarationDetails);
        }
    }

    private void bindQueues(final Channel channel) throws IOException {
        channel.queueBind(ERROR_QUEUE, ERROR_EXCHANGE, "*");
        channel.queueBind(RETRY_QUEUE, RETRY_EXCHANGE, RETRY_QUEUE);
        channel.queueBind(RETRY_WAIT_ENDED_QUEUE, RETRY_EXCHANGE, RETRY_WAIT_ENDED_QUEUE);
        channel.queueBind(DEAD_LETTER_QUEUE, DEAD_LETTER_EXCHANGE, "#");
    }

    private void declareQueue(final QueueDeclarationDetails details) throws IOException {
        final var queueName = details.queueName;
        final var durable = details.durable;
        final var exclusive = details.exclusive;
        final var autoDelete = details.autoDelete;
        final var arguments = details.arguments;

        try (Channel channel = connectionProvider.connect()) {

            final var declareOk = channel.queueDeclare(queueName, durable, exclusive, autoDelete, arguments);
            log.info("Queue declared. name: {}, durable: {}, autoDelete: {}, consumers: {}, message count: {}",
                    declareOk.getQueue(),
                    durable, autoDelete,
                    declareOk.getConsumerCount(),
                    declareOk.getMessageCount());
        } catch (TimeoutException e) {
            log.error("Timeout exception obtaining connection and channel while declaring queue. Queue name: " + queueName, e);
            throw new RuntimeException(e);
        }
    }

    private void declareExchange(final Channel channel, final ExchangeDeclarationDetails details) throws IOException {
        final String exchange = details.exchangeName;
        final BuiltinExchangeType type = details.exchangeType;
        final boolean durable = details.durable;

        channel.exchangeDeclare(exchange, type, durable);
        log.info("Exchange declared. name: {}, type: {}, durable: {}", exchange, type, durable);
    }

    public record QueueDeclarationDetails(String queueName, boolean durable, boolean exclusive, boolean autoDelete,
            Map<String, Object> arguments) {

    }

    public record ExchangeDeclarationDetails(String exchangeName, BuiltinExchangeType exchangeType, boolean durable) {

    }
}
