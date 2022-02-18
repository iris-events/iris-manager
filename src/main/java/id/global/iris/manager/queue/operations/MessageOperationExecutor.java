package id.global.iris.manager.queue.operations;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;

import id.global.iris.manager.connection.ConnectionProvider;
import id.global.iris.manager.queue.utils.MessageChecksum;
import id.global.iris.manager.queue.utils.RoutingMessageHeaderModifier;

@ApplicationScoped
public class MessageOperationExecutor {
    static final long MAX_WAIT_FOR_CONFIRM = 5000;
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageOperationExecutor.class);

    @Inject
    ConnectionProvider connectionProvider;

    @Inject
    MessageChecksum messageChecksum;

    @Inject
    OperationIdGenerator operationIdGenerator;

    @Inject
    RoutingMessageHeaderModifier routingMessageHeaderModifier;

    @Inject
    StateKeepingReturnListenerFactory stateKeepingReturnListenerFactory;

    public void routeAllMessagesAndAcknowledgeOnSuccess(String vhost, String queueName, RouteResolvingFunction fn) {
        consumeAllMessageAndApplyFunctionAndAcknowledgeOnSuccess(vhost, queueName,
                (operationId, channel, response) -> routeMessage(operationId, channel, response, fn));
    }

    public void routeMessagesAndAcknowledgeOnSuccess(String vhost, String queueName, List<String> checksums,
            RouteResolvingFunction fn) {
        consumeAllMessagesAndApplyFunctionAndAcknowledgeOnSuccess(vhost, queueName, checksums,
                (operationId, channel, response) -> routeMessage(operationId, channel, response, fn),
                (operationId, channel, response) -> reinsertMessage(operationId, channel, response, fn));
    }

    public void routeFirstMessageAndAcknowledgeOnSuccess(String vhost, String queueName, String expectedChecksum,
            RouteResolvingFunction fn) {
        consumeMessageAndApplyFunctionAndAcknowledgeOnSuccess(vhost, queueName, expectedChecksum,
                (operationId, channel, response) -> routeMessage(operationId, channel, response, fn));
    }

    public void consumeAllMessageAndApplyFunctionAndAcknowledgeOnSuccess(String vhost, String queueName,
            MessageOperationFunction fn) {

        final OperationId operationId = operationIdGenerator.generate();

        try (final var channel = connectionProvider.connect(vhost)) {
            GetResponse response = channel.basicGet(queueName, false);
            while (massOperationCanBeApplied(response, operationId)) {
                executeMessageOperationAndAckOrNackMessage(fn, operationId, channel, response);

                response = channel.basicGet(queueName, false);
            }
            if (response != null) {
                channel.basicNack(response.getEnvelope().getDeliveryTag(), false, true);
            }
        } catch (MessageOperationFailedException e) {
            throw e;
        } catch (Exception e) {
            throw new MessageOperationFailedException(e);
        }
    }

    public void consumeAllMessagesAndApplyFunctionAndAcknowledgeOnSuccess(String vhost, String queueName,
            List<String> checksums, MessageOperationFunction matchedMessageOperationFunction,
            MessageOperationFunction ignoredMessageOperationFunction) {

        final OperationId operationId = operationIdGenerator.generate();

        try (final var channel = connectionProvider.connect(vhost)) {
            GetResponse response = channel.basicGet(queueName, false);
            while (massOperationCanBeApplied(response, operationId)) {
                String checksum = messageChecksum.createFor(response.getProps(), response.getBody());
                if (checksums.contains(checksum)) {
                    executeMessageOperationAndAckOrNackMessage(matchedMessageOperationFunction, operationId, channel, response);
                } else {
                    executeMessageOperationAndAckOrNackMessage(ignoredMessageOperationFunction, operationId, channel, response);
                }
                response = channel.basicGet(queueName, false);
            }
            if (response != null) {
                channel.basicNack(response.getEnvelope().getDeliveryTag(), false, true);
            }
        } catch (MessageOperationFailedException e) {
            throw e;
        } catch (Exception e) {
            throw new MessageOperationFailedException(e);
        }
    }

    public void consumeMessageAndApplyFunctionAndAcknowledgeOnSuccess(String vhost, String queueName, String expectedChecksum,
            MessageOperationFunction fn) {
        OperationId operationId = operationIdGenerator.generate();
        try (final var channel = connectionProvider.connect(vhost)) {
            GetResponse response = getFirstMessage(queueName, channel);
            String checksum = messageChecksum.createFor(response.getProps(), response.getBody());
            if (checksum.equals(expectedChecksum)) {
                executeMessageOperationAndAckOrNackMessage(fn, operationId, channel, response);
            } else {
                channel.basicNack(response.getEnvelope().getDeliveryTag(), false, true);
                throw new MessageOperationFailedException("Checksum does not match");
            }
        } catch (MessageOperationFailedException e) {
            throw e;
        } catch (Exception e) {
            throw new MessageOperationFailedException(e);
        }
    }

    /**
     * Reinserts message to the end of its current queue. Only used when function is applied on selected messages.
     */
    public void reinsertMessage(OperationId operationId, Channel channel, GetResponse response, RouteResolvingFunction fn)
            throws IOException, TimeoutException, InterruptedException {
        RoutingDetails routingDetails = fn.resolve(response);

        StateKeepingReturnListener returnListener = stateKeepingReturnListenerFactory.createFor(operationId, LOGGER);
        channel.addReturnListener(returnListener);
        channel.confirmSelect();

        final var props = routingMessageHeaderModifier.modifyHeaders(response.getProps(), operationId,
                routingDetails != null ? routingDetails.countHeaderName() : null);
        final var currentMessageExchange = response.getEnvelope().getExchange();
        final var currentMessageRoutingKey = response.getEnvelope().getRoutingKey();
        channel.basicPublish(currentMessageExchange, currentMessageRoutingKey, true, props, response.getBody());
        channel.waitForConfirmsOrDie(MAX_WAIT_FOR_CONFIRM);

        if (returnListener.isReceived()) {
            throw new MessageOperationFailedException("Failed to perform operation, basic.return received");
        }

        channel.removeReturnListener(returnListener);
    }

    private void routeMessage(OperationId operationId, Channel channel, GetResponse response, RouteResolvingFunction fn)
            throws IOException, TimeoutException, InterruptedException {
        RoutingDetails routingDetails = fn.resolve(response);

        StateKeepingReturnListener returnListener = stateKeepingReturnListenerFactory.createFor(operationId, LOGGER);
        channel.addReturnListener(returnListener);
        channel.confirmSelect();

        AMQP.BasicProperties props = routingMessageHeaderModifier.modifyHeaders(response.getProps(), operationId,
                routingDetails.countHeaderName());
        channel.basicPublish(routingDetails.exchange(), routingDetails.routingKey(), true, props, response.getBody());
        channel.waitForConfirmsOrDie(MAX_WAIT_FOR_CONFIRM);
        if (returnListener.isReceived()) {
            throw new MessageOperationFailedException("Failed to perform operation, basic.return received");
        }

        channel.removeReturnListener(returnListener);
    }

    private boolean massOperationCanBeApplied(GetResponse getResponse, OperationId operationId) {
        if (getResponse == null) {
            return false;
        }
        if (getResponse.getProps() == null || getResponse.getProps().getHeaders() == null) {
            return true;
        }

        final var header = getResponse.getProps().getHeaders().getOrDefault(OperationId.HEADER_NAME, "undefined")
                .toString();
        return !operationId.equals(header);
    }

    private void executeMessageOperationAndAckOrNackMessage(MessageOperationFunction fn, OperationId operationId,
            Channel channel, GetResponse response) throws IOException, TimeoutException, InterruptedException {
        try {
            fn.apply(operationId, channel, response);
            channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
        } catch (Exception e) {
            channel.basicNack(response.getEnvelope().getDeliveryTag(), false, true);
            throw e;
        }
    }

    private GetResponse getFirstMessage(String queue, Channel channel) throws IOException {
        GetResponse response = channel.basicGet(queue, false);
        if (response != null) {
            return response;
        }
        throw new MessageOperationFailedException("No message in queue");
    }
}
