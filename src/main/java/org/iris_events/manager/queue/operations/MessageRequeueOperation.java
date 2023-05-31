package org.iris_events.manager.queue.operations;

import java.util.List;
import java.util.Map;

import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.LongString;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class MessageRequeueOperation {
    public static final String X_DEATH_HEADER_KEY_NAME = "x-death";
    public static final String X_DEATH_EXCHANGE_KEY_NAME = "exchange";
    public static final String X_DEATH_ROUTING_KEYS_KEY_NAME = "routing-keys";
    public static final String REQUEUE_COUNT_HEADER = "x-rmqmgmt-requeue-count";

    private final MessageOperationExecutor messageOperationExecutor;

    @Inject
    public MessageRequeueOperation(MessageOperationExecutor messageOperationExecutor) {
        this.messageOperationExecutor = messageOperationExecutor;
    }

    public void requeueAllMessages(String vhost, String queueName) {
        messageOperationExecutor.routeAllMessagesAndAcknowledgeOnSuccess(vhost, queueName, this::getRoutingDetailsFromMessage);
    }

    public void requeueFirstMessage(String vhost, String queueName, String checksum) {
        messageOperationExecutor.routeFirstMessageAndAcknowledgeOnSuccess(vhost, queueName, checksum,
                this::getRoutingDetailsFromMessage);
    }

    public void requeueMultipleMessages(String vhost, String queueName, List<String> checksums) {
        messageOperationExecutor.routeMessagesAndAcknowledgeOnSuccess(vhost, queueName, checksums,
                this::getRoutingDetailsFromMessage);
    }

    private RoutingDetails getRoutingDetailsFromMessage(GetResponse response) {
        if (response.getProps().getHeaders() == null || !response.getProps().getHeaders()
                .containsKey(X_DEATH_HEADER_KEY_NAME)) {
            throw new MessageOperationFailedException("Requeue operation not available; x-death header missing");
        }

        List<Map<String, Object>> xDeath = (List<Map<String, Object>>) response.getProps().getHeaders()
                .get(X_DEATH_HEADER_KEY_NAME);
        if (xDeath.isEmpty()) {
            throw new MessageOperationFailedException("Requeue operation not available; x-death header missing");
        }

        Map<String, Object> firstEntry = xDeath.get(0);
        if (!firstEntry.containsKey(X_DEATH_EXCHANGE_KEY_NAME)) {
            throw new MessageOperationFailedException("Requeue operation not available; exchange is missing");
        }

        String targetExchange = firstEntry.get(X_DEATH_EXCHANGE_KEY_NAME).toString();
        if (!firstEntry.containsKey(X_DEATH_ROUTING_KEYS_KEY_NAME)) {
            throw new MessageOperationFailedException("Requeue operation not available; routing keys are missing");
        }
        List<LongString> targetRoutingKeys = (List<LongString>) firstEntry.get(X_DEATH_ROUTING_KEYS_KEY_NAME);
        if (targetRoutingKeys.isEmpty()) {
            throw new MessageOperationFailedException("Requeue operation not available; routing keys are missing");
        }

        return new RoutingDetails(targetExchange, targetRoutingKeys.get(0).toString(), REQUEUE_COUNT_HEADER);
    }

}
