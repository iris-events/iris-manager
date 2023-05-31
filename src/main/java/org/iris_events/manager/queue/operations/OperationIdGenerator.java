package org.iris_events.manager.queue.operations;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class OperationIdGenerator {

    public OperationId generate() {
        return new OperationId();
    }

}
