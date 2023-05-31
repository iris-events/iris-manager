package org.iris_events.manager.queue.operations;

public class MessageFetchFailedException extends RuntimeException {

    public MessageFetchFailedException(Exception cause) {
        super("Failed to fetch messages from RabbitMQ", cause);
    }
}
