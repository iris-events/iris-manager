package org.iris_events.manager.queue.operations;

import java.util.UUID;

import io.smallrye.common.constraint.NotNull;


public record OperationId(String value) {

    public static final String HEADER_NAME = "x-rmqmgmt-routing-operation-id";

    public OperationId() {
        this(generateValue());
    }

    @NotNull
    private static String generateValue() {
        return UUID.randomUUID().toString().replaceAll("-", "");
    }

    @Override
    public String toString() {
        return value;
    }

    public boolean equals(String o) {
        return value.equals(o);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        OperationId that = (OperationId) o;
        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }
}
