package id.global.iris.manager.queue.utils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rabbitmq.client.AMQP.BasicProperties;

@ApplicationScoped
public class MessageChecksum {

    static final String BODY_KEY = "body";
    static final String PROPERTIES_KEY = "properties";

    private final ObjectMapper objectMapper;

    @Inject
    public MessageChecksum(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        this.objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    public String createFor(BasicProperties props, byte[] body) {
        String json = toJson(props, body);
        MessageDigest digest = getMessageDigest();
        byte[] encodedHash = digest.digest(json.getBytes(StandardCharsets.UTF_8));

        return bytesToHex(encodedHash);
    }

    private MessageDigest getMessageDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private static String bytesToHex(byte[] hash) {
        StringBuilder hexString = new StringBuilder(2 * hash.length);
        for (final byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }

        return hexString.toString();
    }

    String toJson(BasicProperties props, byte[] body) {
        ObjectNode data = objectMapper.createObjectNode();
        data.set(PROPERTIES_KEY, objectMapper.valueToTree(props));
        data.put(BODY_KEY, encode(body));
        try {
            return objectMapper.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String encode(byte[] body) {
        return new String(Base64.getEncoder().encode(body), StandardCharsets.UTF_8);
    }

}
