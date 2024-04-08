package org.iris_events.manager.infrastructure;

import static org.iris_events.common.MessagingHeaders.QueueDeclaration.X_MESSAGE_TTL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;

import io.quarkus.test.junit.DisabledOnIntegrationTest;
import org.iris_events.manager.connection.ConnectionProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;

import org.iris_events.common.Exchanges;
import org.iris_events.common.Queues;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.InjectMock;
import jakarta.inject.Inject;

@QuarkusTest
@DisabledOnIntegrationTest(value = "This test requires RabbitMQ", forArtifactTypes = {DisabledOnIntegrationTest.ArtifactType.CONTAINER, DisabledOnIntegrationTest.ArtifactType.NATIVE_BINARY})
class InfrastructureDeclaratorTest {

    @Inject
    InfrastructureDeclarator infrastructureDeclarator;

    @InjectMock
    ConnectionProvider connectionProvider;

    private Channel channel;

    @BeforeEach
    void setUp() throws Exception {
        channel = mock(Channel.class);
        final var declareOk = mock(AMQP.Queue.DeclareOk.class);
        when(channel.queueDeclare(anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any())).thenReturn(declareOk);
        when(connectionProvider.connect()).thenReturn(channel);
    }

    @Test
    void declareBackboneInfrastructure() throws Exception {
        infrastructureDeclarator.declareBackboneInfrastructure();

        verify(channel).exchangeDeclare(Exchanges.ERROR.getValue(), BuiltinExchangeType.TOPIC, true);
        verify(channel).exchangeDeclare(Exchanges.RETRY.getValue(), BuiltinExchangeType.DIRECT, true);
        verify(channel).exchangeDeclare(Exchanges.DEAD_LETTER.getValue(), BuiltinExchangeType.TOPIC, true);
        verify(channel).exchangeDeclare(Exchanges.FRONTEND.getValue(), BuiltinExchangeType.TOPIC, true);
        verify(channel).exchangeDeclare(Exchanges.SESSION.getValue(), BuiltinExchangeType.TOPIC, true);
        verify(channel).exchangeDeclare(Exchanges.USER.getValue(), BuiltinExchangeType.TOPIC, true);
        verify(channel).exchangeDeclare(Exchanges.BROADCAST.getValue(), BuiltinExchangeType.TOPIC, true);
        verify(channel).exchangeDeclare(Exchanges.SUBSCRIPTION.getValue(), BuiltinExchangeType.TOPIC, true);
        verify(channel).exchangeDeclare(Exchanges.SNAPSHOT_REQUESTED.getValue(), BuiltinExchangeType.TOPIC, true);

        verify(channel).queueDeclare(Queues.ERROR.getValue(), true, false, false, null);
        verify(channel).queueDeclare(Queues.RETRY.getValue(), true, false, false, Map.of(X_MESSAGE_TTL, 5000));
        verify(channel).queueDeclare(Queues.RETRY_WAIT_ENDED.getValue(), true, false, false, null);
        verify(channel).queueDeclare(Queues.DEAD_LETTER.getValue(), true, false, false, null);
        verify(channel).queueDeclare(Queues.SUBSCRIPTION.getValue(), true, false, false, null);

        verify(channel).queueBind(Queues.ERROR.getValue(), Exchanges.ERROR.getValue(), "*");
        verify(channel).queueBind(Queues.RETRY.getValue(), Exchanges.RETRY.getValue(), Queues.RETRY.getValue());
        verify(channel).queueBind(Queues.RETRY_WAIT_ENDED.getValue(), Exchanges.RETRY.getValue(),
                Queues.RETRY_WAIT_ENDED.getValue());
        verify(channel).queueBind(Queues.DEAD_LETTER.getValue(), Exchanges.DEAD_LETTER.getValue(), "#");

        verifyNoMoreInteractions(channel);
    }

}
