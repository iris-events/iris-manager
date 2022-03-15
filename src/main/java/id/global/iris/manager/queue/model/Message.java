package id.global.iris.manager.queue.model;

import static id.global.iris.manager.queue.operations.MessageMoveOperation.MOVE_COUNT_HEADER;
import static id.global.iris.manager.queue.operations.MessageRequeueOperation.REQUEUE_COUNT_HEADER;

import java.util.List;
import java.util.Map;

import com.rabbitmq.client.Envelope;

public class Message {
    public static final String X_DEATH_HEADER = "x-death";
    public static final String XDEATH_HEADER_EXCHANGE_FIELD = "exchange";
    public static final String XDEATH_HEADER_ROUTING_KEYS_FIELD = "routing-keys";
    private final Envelope envelope;
    private final BasicProperties properties;
    private final byte[] body;
    private final String checksum;
    private RequeueDetails requeueDetails;

    public Message(Envelope envelope, BasicProperties properties, byte[] body, String checksum) {
        this.envelope = envelope;
        this.properties = properties;
        this.body = body;
        this.checksum = checksum;

        if (properties.getHeaders() != null && properties.getHeaders().get(X_DEATH_HEADER) != null) {
            List xDeathList = (List) (properties.getHeaders().get(X_DEATH_HEADER));
            if (!xDeathList.isEmpty()) {
                Map xdeath = (Map) xDeathList.get(0);
                String exchange = (String) xdeath.get(XDEATH_HEADER_EXCHANGE_FIELD);
                String routingKey = (String) (xdeath.get(XDEATH_HEADER_ROUTING_KEYS_FIELD) != null && !((List) xdeath.get(
                        XDEATH_HEADER_ROUTING_KEYS_FIELD)).isEmpty()
                                ? ((List) xdeath.get(XDEATH_HEADER_ROUTING_KEYS_FIELD)).get(0)
                                : null);
                if (hasText(exchange) && hasText(routingKey)) {
                    requeueDetails = new RequeueDetails(exchange, routingKey);
                }
            }
        }
    }

    private static boolean hasText(final String text) {
        if (text == null) {
            return false;
        }

        return !text.isBlank() && !text.isEmpty();
    }

    public Envelope getEnvelope() {
        return envelope;
    }

    public BasicProperties getProperties() {
        return properties;
    }

    public byte[] getBody() {
        return body;
    }

    public String getChecksum() {
        return checksum;
    }

    public RequeueDetails getRequeueDetails() {
        return requeueDetails;
    }

    public boolean isRequeueAllowed() {
        return requeueDetails != null;
    }

    public boolean isRequeued() {
        return getRequeueCount() > 0;
    }

    public int getRequeueCount() {
        return getCount(REQUEUE_COUNT_HEADER);
    }

    public boolean isMoved() {
        return getMovedCount() > 0;
    }

    public int getMovedCount() {
        return getCount(MOVE_COUNT_HEADER);
    }

    private int getCount(final String requeueCountHeader) {
        return properties.getHeaders() != null ? (int) properties.getHeaders().getOrDefault(requeueCountHeader, 0) : 0;
    }

    public record RequeueDetails(
            String exchangeName,
            String routingKey) {
    }
}
