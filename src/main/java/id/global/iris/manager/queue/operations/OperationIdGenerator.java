package id.global.iris.manager.queue.operations;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class OperationIdGenerator {

    public OperationId generate() {
        return new OperationId();
    }

}
