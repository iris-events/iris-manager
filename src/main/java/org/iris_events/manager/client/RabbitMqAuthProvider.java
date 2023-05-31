package org.iris_events.manager.client;

import java.util.Base64;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.client.ClientRequestFilter;

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
