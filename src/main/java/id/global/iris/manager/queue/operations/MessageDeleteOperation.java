package id.global.iris.manager.queue.operations;

import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class MessageDeleteOperation {
    private final MessageOperationExecutor messageOperationExecutor;

    @Inject
    public MessageDeleteOperation(MessageOperationExecutor messageOperationExecutor) {
        this.messageOperationExecutor = messageOperationExecutor;
    }

    public void deleteFirstMessageInQueue(String vhost, String queueName, String messageChecksum) {
        messageOperationExecutor.consumeMessageAndApplyFunctionAndAcknowledgeOnSuccess(vhost, queueName, messageChecksum,
                (i, c, r) -> {
                });
    }

    public void deleteMultipleMessagesInQueue(String vhost, String queueName, List<String> checksums) {
        messageOperationExecutor.consumeAllMessagesAndApplyFunctionAndAcknowledgeOnSuccess(vhost, queueName, checksums,
                (i, c, r) -> {
                },
                (i, c, r) -> messageOperationExecutor.reinsertMessage(i, c, r, m -> new RoutingDetails(null, null, null)));
    }
}
