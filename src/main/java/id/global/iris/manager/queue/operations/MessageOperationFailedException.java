package id.global.iris.manager.queue.operations;

public class MessageOperationFailedException extends RuntimeException {
    private static final String MESSAGE = "Failed to perform operation on message";

    public MessageOperationFailedException(String details) {
        super(MESSAGE + ": " + details);
    }

    public MessageOperationFailedException(Throwable cause) {
        super(MESSAGE, cause);
    }
}
