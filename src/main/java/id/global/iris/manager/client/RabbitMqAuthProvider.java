package id.global.iris.manager.client;

import java.util.Base64;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.ClientRequestFilter;

import org.eclipse.microprofile.config.inject.ConfigProperty;

@Singleton
public class RabbitMqAuthProvider implements ClientRequestFilter {

    private String authHeader;

    @ConfigProperty(name = "quarkus.rabbitmqclient.username")
    String username;

    @ConfigProperty(name = "quarkus.rabbitmqclient.password")
    String password;

    @Inject
    void setup() {
        authHeader = "Basic " + Base64.getEncoder()
                .encodeToString((username + ":" + password).getBytes());
    }

    @Override
    public void filter(ClientRequestContext requestContext) {
        if (requestContext.getHeaders().containsKey("Authorization")) {
            return;
        }
        requestContext.getHeaders().putSingle("Authorization", authHeader);
    }
}
