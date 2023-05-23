package id.global.iris.manager.queue.operations;

import java.util.List;

import id.global.iris.manager.queue.model.Message;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class Operations {
    private final QueueListOperation queueListOperation;
    private final MessageMoveOperation messageMoveOperation;
    private final MessageRequeueOperation messageRequeueOperation;
    private final MessageDeleteOperation messageDeleteOperation;

    @Inject
    public Operations(QueueListOperation queueListOperation,
            MessageMoveOperation messageMoveOperation,
            MessageRequeueOperation messageRequeueOperation,
            MessageDeleteOperation messageDeleteOperation) {
        this.queueListOperation = queueListOperation;
        this.messageMoveOperation = messageMoveOperation;
        this.messageRequeueOperation = messageRequeueOperation;
        this.messageDeleteOperation = messageDeleteOperation;
    }

    public List<Message> getMessagesOfQueue(String vhost, String queueName, int limit) {
        return queueListOperation.getMessagesFromQueue(vhost, queueName, limit);
    }

    public void deleteFirstMessageInQueue(String vhost, String queueName, String messageChecksum) {
        messageDeleteOperation.deleteFirstMessageInQueue(vhost, queueName, messageChecksum);
    }

    public void deleteMultipleMessagesInQueue(String vhost, String queueName, List<String> checksums) {
        messageDeleteOperation.deleteMultipleMessagesInQueue(vhost, queueName, checksums);
    }

    public void moveAllMessagesInQueue(String vhost, String queueName, String targetExchange, String targetRoutingKey) {
        messageMoveOperation.moveAllMessages(vhost, queueName, targetExchange, targetRoutingKey);
    }

    public void moveMultipleMessagesInQueue(String vhost, String queueName, List<String> checksums, String targetExchange,
            String targetRoutingKey) {
        messageMoveOperation.moveMultipleMessages(vhost, queueName, checksums, targetExchange, targetRoutingKey);
    }

    public void moveFirstMessageInQueue(String vhost, String queueName, String messageChecksum, String targetExchange,
            String targetRoutingKey) {
        messageMoveOperation.moveFirstMessage(vhost, queueName, messageChecksum, targetExchange, targetRoutingKey);
    }

    public void requeueAllMessagesInQueue(String vhost, String queueName) {
        messageRequeueOperation.requeueAllMessages(vhost, queueName);
    }

    public void requeueMultipleMessagesInQueue(String vhost, String queueName, List<String> checksums) {
        messageRequeueOperation.requeueMultipleMessages(vhost, queueName, checksums);
    }

    public void requeueFirstMessageInQueue(String vhost, String queueName, String messageChecksum) {
        messageRequeueOperation.requeueFirstMessage(vhost, queueName, messageChecksum);
    }

}
