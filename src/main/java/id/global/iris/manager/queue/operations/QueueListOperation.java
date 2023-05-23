package id.global.iris.manager.queue.operations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.GetResponse;

import id.global.iris.manager.connection.ConnectionProvider;
import id.global.iris.manager.queue.model.Message;
import id.global.iris.manager.queue.utils.MessageMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class QueueListOperation {
    public static final int DEFAULT_FETCH_COUNT = 10;

    private final ConnectionProvider connectionProvider;
    private final MessageMapper messageMapper;

    @Inject
    public QueueListOperation(ConnectionProvider connectionProvider, MessageMapper messageMapper) {
        this.connectionProvider = connectionProvider;
        this.messageMapper = messageMapper;
    }

    public List<Message> getMessagesFromQueue(String vhost, String queueName, int maxNumberOfMessages) {
        try (final var channel = connectionProvider.connect(vhost)) {
            List<Message> messages = new ArrayList<>();
            channel.basicQos(DEFAULT_FETCH_COUNT);
            int fetched = 0;
            boolean messagesAvailable = true;
            Long lastDeliveryTag = null;
            while (fetched < maxNumberOfMessages && messagesAvailable) {
                GetResponse response = channel.basicGet(queueName, false);
                if (response != null) {
                    messages.add(createMessage(response));
                    lastDeliveryTag = response.getEnvelope().getDeliveryTag();
                    fetched++;
                    messagesAvailable = response.getMessageCount() > 0;
                } else {
                    messagesAvailable = false;
                }
            }
            if (lastDeliveryTag != null) {
                channel.basicNack(lastDeliveryTag, true, true);
            }
            return messages;
        } catch (IOException | TimeoutException e) {
            throw new MessageFetchFailedException(e);
        }
    }

    private Message createMessage(GetResponse response) {
        return messageMapper.map(response);
    }

}
