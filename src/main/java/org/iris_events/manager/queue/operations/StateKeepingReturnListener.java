package org.iris_events.manager.queue.operations;

import java.io.IOException;

import org.slf4j.Logger;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ReturnListener;

class StateKeepingReturnListener implements ReturnListener {
    private final OperationId operation;
    private final Logger logger;

    private boolean received;

    StateKeepingReturnListener(OperationId operation, Logger logger) {
        this.operation = operation;
        this.logger = logger;
    }

    @Override
    public void handleReturn(int replyCode, String replyText, String exchange, String routingKey,
            AMQP.BasicProperties properties, byte[] body) throws IOException {
        logger.error("basic.return received for operation with id {}: exchange={}, routingKey={}, replyCode={}, replyText={}",
                operation, exchange, routingKey, replyCode, replyText);
        received = true;
    }

    boolean isReceived() {
        return received;
    }
}
