package id.global.iris.manager.queue.operations;

import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class MessageMoveOperation {
    public static final String MOVE_COUNT_HEADER = "x-rmqmgmt-move-count";

    private final MessageOperationExecutor messageOperationExecutor;

    @Inject
    public MessageMoveOperation(MessageOperationExecutor messageOperationExecutor) {
        this.messageOperationExecutor = messageOperationExecutor;
    }

    public void moveAllMessages(String vhost, String queueName, String targetExchange, String targetRoutingKey) {
        messageOperationExecutor.routeAllMessagesAndAcknowledgeOnSuccess(vhost, queueName,
                m -> new RoutingDetails(targetExchange, targetRoutingKey, MOVE_COUNT_HEADER));
    }

    public void moveMultipleMessages(String vhost, String queueName, List<String> checksums, String targetExchange,
            String targetRoutingKey) {
        messageOperationExecutor.routeMessagesAndAcknowledgeOnSuccess(vhost, queueName, checksums,
                m -> new RoutingDetails(targetExchange, targetRoutingKey, MOVE_COUNT_HEADER));
    }

    public void moveFirstMessage(String vhost, String queueName, String checksum, String targetExchange,
            String targetRoutingKey) {
        messageOperationExecutor.routeFirstMessageAndAcknowledgeOnSuccess(vhost, queueName, checksum,
                m -> new RoutingDetails(targetExchange, targetRoutingKey, MOVE_COUNT_HEADER));
    }

}
