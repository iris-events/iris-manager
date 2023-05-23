package id.global.iris.manager.queue.operations;

import org.slf4j.Logger;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class StateKeepingReturnListenerFactory {

    public StateKeepingReturnListener createFor(OperationId operationId, Logger logger) {
        return new StateKeepingReturnListener(operationId, logger);
    }

}
