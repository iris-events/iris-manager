package id.global.iris.manager.queue.operations;

import javax.enterprise.context.ApplicationScoped;

import org.slf4j.Logger;

@ApplicationScoped
public class StateKeepingReturnListenerFactory {

    public StateKeepingReturnListener createFor(OperationId operationId, Logger logger) {
        return new StateKeepingReturnListener(operationId, logger);
    }

}
