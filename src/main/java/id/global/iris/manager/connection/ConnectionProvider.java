package id.global.iris.manager.connection;

import java.io.IOException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import id.global.iris.manager.InstanceInfoProvider;
import io.quarkiverse.rabbitmqclient.RabbitMQClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class ConnectionProvider {

    private static final String DEFAULT_VHOST = "/";

    private final RabbitMQClient rabbitMQClient;
    private final String applicationName;
    private final String instanceName;

    @Inject
    public ConnectionProvider(final RabbitMQClient rabbitMQClient, final InstanceInfoProvider instanceInfoProvider) {
        this.rabbitMQClient = rabbitMQClient;
        this.applicationName = instanceInfoProvider.getApplicationName();
        this.instanceName = instanceInfoProvider.getInstanceName();
    }

    public Channel connect() throws IOException {
        final var connection = getConnection(DEFAULT_VHOST);
        return connection.createChannel();
    }

    public Channel connect(final String vhost) throws IOException {
        final var connection = getConnection(vhost);
        return connection.createChannel();
    }

    private Connection getConnection(final String vhost) {
        final var connectionName = String.format("%s-%s-%s", applicationName, instanceName, vhost);
        return rabbitMQClient.connect(connectionName);
    }
}
