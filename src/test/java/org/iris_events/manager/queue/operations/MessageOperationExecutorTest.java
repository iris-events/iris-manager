package org.iris_events.manager.queue.operations;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.iris_events.manager.connection.ConnectionProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.slf4j.Logger;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;

import org.iris_events.manager.queue.utils.MessageChecksum;
import org.iris_events.manager.queue.utils.RoutingMessageHeaderModifier;
import io.quarkiverse.rabbitmqclient.RabbitMQClientException;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.InjectMock;
import jakarta.inject.Inject;

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MessageOperationExecutorTest {

    private static final String DEFAULT_VHOST_NAME = "defaultVhost";
    private static final String DEFAULT_QUEUE_NAME = "defaultQueue";
    private static final Envelope DEFAULT_ENVELOPE = mock(Envelope.class);
    private static final Long DEFAULT_DELIVERY_TAG = 123L;
    private static final AMQP.BasicProperties DEFAULT_BASIC_PROPERTIES = mock(AMQP.BasicProperties.class);
    private static final byte[] DEFAULT_PAYLOAD = "defaultPayload".getBytes(StandardCharsets.UTF_8);
    private static final String DEFAULT_CHECKSUM = "defaultChecksum";
    private static final String DEFAULT_ROUTING_TARGET_EXCHANGE = "targetExchange";
    private static final String DEFAULT_ROUTING_TARGET_ROUTING_KEY = "targetRoutingKey";
    private static final String DEFAULT_ROUTING_COUNT_HEADER = "countHeader";
    private static final OperationId DEFAULT_OPERATION_ID = new OperationId();

    @Inject
    MessageOperationExecutor sut;

    @InjectMock
    private ConnectionProvider connectionProvider;
    @InjectMock
    private MessageChecksum messageChecksum;
    @InjectMock
    private OperationIdGenerator operationIdGenerator;
    @InjectMock
    private RoutingMessageHeaderModifier routingMessageHeaderModifier;
    @InjectMock
    private StateKeepingReturnListenerFactory stateKeepingReturnListenerFactory;

    private Channel channel;
    private MessageOperationFunction basicFunction;
    private RouteResolvingFunction routeResolvingFunction;
    private StateKeepingReturnListener stateKeepingReturnListener;

    @BeforeEach
    void init() throws IOException {
        channel = mock(Channel.class);
        basicFunction = mock(MessageOperationFunction.class);
        routeResolvingFunction = mock(RouteResolvingFunction.class);
        stateKeepingReturnListener = mock(StateKeepingReturnListener.class);
        when(connectionProvider.connect(DEFAULT_VHOST_NAME)).thenReturn(channel);
        when(DEFAULT_ENVELOPE.getDeliveryTag()).thenReturn(DEFAULT_DELIVERY_TAG);
        when(operationIdGenerator.generate()).thenReturn(DEFAULT_OPERATION_ID);
    }

    @Test
    void shouldRetrieveMessageFromQueueAndPerformFunctionWhenChecksumMatches() throws Exception {
        GetResponse response = mockDefaultGetResponse();

        when(messageChecksum.createFor(DEFAULT_BASIC_PROPERTIES, DEFAULT_PAYLOAD)).thenReturn(DEFAULT_CHECKSUM);
        when(channel.basicGet(DEFAULT_QUEUE_NAME, false)).thenReturn(response);

        sut.consumeMessageAndApplyFunctionAndAcknowledgeOnSuccess(DEFAULT_VHOST_NAME, DEFAULT_QUEUE_NAME, DEFAULT_CHECKSUM,
                basicFunction);

        verify(basicFunction).apply(DEFAULT_OPERATION_ID, channel, response);
        verify(channel).basicAck(DEFAULT_DELIVERY_TAG, false);
    }

    @Test
    void shouldRetrieveMessageFromQueueAndNackWithRequeuWhenChecksumDoesNotMatch() throws Exception {
        GetResponse response = mockDefaultGetResponse();

        when(messageChecksum.createFor(DEFAULT_BASIC_PROPERTIES, DEFAULT_PAYLOAD)).thenReturn(DEFAULT_CHECKSUM);
        when(channel.basicGet(DEFAULT_QUEUE_NAME, false)).thenReturn(response);

        try {
            sut.consumeMessageAndApplyFunctionAndAcknowledgeOnSuccess(DEFAULT_VHOST_NAME, DEFAULT_QUEUE_NAME, "invalidChecksum",
                    basicFunction);
            fail();
        } catch (MessageOperationFailedException ignored) {
        }

        verify(channel).basicNack(DEFAULT_DELIVERY_TAG, false, true);
        verify(basicFunction, never()).apply(any(OperationId.class), any(Channel.class), any(GetResponse.class));
    }

    @Test
    void shouldFailWhenQueueIsEmpty() throws Exception {
        when(channel.basicGet(DEFAULT_QUEUE_NAME, false)).thenReturn(null);

        try {
            sut.consumeMessageAndApplyFunctionAndAcknowledgeOnSuccess(DEFAULT_VHOST_NAME, DEFAULT_QUEUE_NAME, "anyChecksum",
                    basicFunction);
            fail();
        } catch (MessageOperationFailedException ignored) {
        }

        verify(channel, never()).basicNack(any(Long.class), anyBoolean(), anyBoolean());
        verify(basicFunction, never()).apply(any(OperationId.class), any(Channel.class), any(GetResponse.class));
    }

    @Test
    void shouldThrowExceptionWhenConnectionCannotBeEstablished() throws Exception {
        RabbitMQClientException expectedException = new RabbitMQClientException("Failed to connect to RabbitMQ broker", null);

        when(connectionProvider.connect(DEFAULT_VHOST_NAME)).thenThrow(expectedException);

        try {
            sut.consumeMessageAndApplyFunctionAndAcknowledgeOnSuccess(DEFAULT_VHOST_NAME, DEFAULT_QUEUE_NAME, DEFAULT_CHECKSUM,
                    basicFunction);
        } catch (MessageOperationFailedException e) {
            assertSame(expectedException, e.getCause());
        }

        verify(basicFunction, never()).apply(any(OperationId.class), any(Channel.class), any(GetResponse.class));
    }

    @Test
    void shouldThrowExceptionWhenMessageCannotBeFetched() throws Exception {
        IOException expectedException = new IOException();

        when(channel.basicGet(DEFAULT_QUEUE_NAME, false)).thenThrow(expectedException);

        try {
            sut.consumeMessageAndApplyFunctionAndAcknowledgeOnSuccess(DEFAULT_VHOST_NAME, DEFAULT_QUEUE_NAME, DEFAULT_CHECKSUM,
                    basicFunction);
        } catch (MessageOperationFailedException e) {
            assertSame(expectedException, e.getCause());
        }

        verify(channel).basicGet(DEFAULT_QUEUE_NAME, false);
        verify(channel).close();
        verifyNoMoreInteractions(channel);
        verify(basicFunction, never()).apply(any(OperationId.class), any(Channel.class), any(GetResponse.class));
    }

    @Test
    void shouldThrowExceptionWhenFunctionCannotBePerformedSuccessfulChecksumCheck()
            throws Exception {
        GetResponse getResponse = mockDefaultGetResponse();
        IOException expectedException = new IOException();

        when(messageChecksum.createFor(DEFAULT_BASIC_PROPERTIES, DEFAULT_PAYLOAD)).thenReturn(DEFAULT_CHECKSUM);
        when(channel.basicGet(DEFAULT_QUEUE_NAME, false)).thenReturn(getResponse);
        doThrow(expectedException).when(basicFunction).apply(DEFAULT_OPERATION_ID, channel, getResponse);

        try {
            sut.consumeMessageAndApplyFunctionAndAcknowledgeOnSuccess(DEFAULT_VHOST_NAME, DEFAULT_QUEUE_NAME, DEFAULT_CHECKSUM,
                    basicFunction);
        } catch (MessageOperationFailedException e) {
            assertSame(expectedException, e.getCause());
        }

        verify(messageChecksum).createFor(DEFAULT_BASIC_PROPERTIES, DEFAULT_PAYLOAD);
        verify(channel).basicGet(DEFAULT_QUEUE_NAME, false);
        verify(basicFunction).apply(DEFAULT_OPERATION_ID, channel, getResponse);
        verify(channel).basicNack(DEFAULT_DELIVERY_TAG, false, true);
        verify(channel).close();
        verifyNoMoreInteractions(channel);
    }

    @Test
    void shouldThrowExceptionWhenAckCannotBeSentAfterSuccessfulFunctionExecution() throws Exception {
        GetResponse getResponse = mockDefaultGetResponse();
        IOException expectedException = new IOException();

        when(messageChecksum.createFor(DEFAULT_BASIC_PROPERTIES, DEFAULT_PAYLOAD)).thenReturn(DEFAULT_CHECKSUM);
        when(channel.basicGet(DEFAULT_QUEUE_NAME, false)).thenReturn(getResponse);
        doThrow(expectedException).when(channel).basicAck(DEFAULT_DELIVERY_TAG, false);

        try {
            sut.consumeMessageAndApplyFunctionAndAcknowledgeOnSuccess(DEFAULT_VHOST_NAME, DEFAULT_QUEUE_NAME, DEFAULT_CHECKSUM,
                    basicFunction);
        } catch (MessageOperationFailedException e) {
            assertSame(expectedException, e.getCause());
        }

        verify(messageChecksum).createFor(DEFAULT_BASIC_PROPERTIES, DEFAULT_PAYLOAD);
        verify(channel).basicGet(DEFAULT_QUEUE_NAME, false);
        verify(basicFunction).apply(DEFAULT_OPERATION_ID, channel, getResponse);
        verify(channel).basicAck(DEFAULT_DELIVERY_TAG, false);
        verify(channel).basicNack(DEFAULT_DELIVERY_TAG, false, true);
        verify(channel).close();
        verifyNoMoreInteractions(channel);
    }

    @Test
    void shouldThrowExceptionWhenNackCannotBeSentAfterChecksumMatchFailed()
            throws Exception {
        GetResponse getResponse = mockDefaultGetResponse();
        IOException expectedException = new IOException();

        when(messageChecksum.createFor(DEFAULT_BASIC_PROPERTIES, DEFAULT_PAYLOAD)).thenReturn(DEFAULT_CHECKSUM);
        when(channel.basicGet(DEFAULT_QUEUE_NAME, false)).thenReturn(getResponse);
        doThrow(expectedException).when(channel).basicNack(DEFAULT_DELIVERY_TAG, false, true);

        try {
            sut.consumeMessageAndApplyFunctionAndAcknowledgeOnSuccess(DEFAULT_VHOST_NAME, DEFAULT_QUEUE_NAME, "invalidMessage",
                    basicFunction);
        } catch (MessageOperationFailedException e) {
            assertSame(expectedException, e.getCause());
        }

        verify(messageChecksum).createFor(DEFAULT_BASIC_PROPERTIES, DEFAULT_PAYLOAD);
        verify(channel).basicGet(DEFAULT_QUEUE_NAME, false);
        verify(channel).basicNack(DEFAULT_DELIVERY_TAG, false, true);
        verify(channel).close();
        verifyNoMoreInteractions(channel);
        verify(basicFunction, never()).apply(any(OperationId.class), any(Channel.class), any(GetResponse.class));
    }

    @Test
    void shouldPerformMassOperationUntilAllMessagesAreConsumedAndMessagesDoNotContainAnyBasicProperty() throws Exception {
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.getEnvelope()).thenReturn(DEFAULT_ENVELOPE);

        testSuccessfulMassOperationForMessage(getResponse);
    }

    @Test
    void shouldPerformMassOperationUntilAllMessagesAreConsumedAndMessagesDoNotContainAnyHeader() throws Exception {
        AMQP.BasicProperties basicProperties = mock(AMQP.BasicProperties.class);
        when(basicProperties.getHeaders()).thenReturn(null);
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.getEnvelope()).thenReturn(DEFAULT_ENVELOPE);
        when(getResponse.getProps()).thenReturn(basicProperties);

        testSuccessfulMassOperationForMessage(getResponse);
    }

    @Test
    void shouldPerformMassOperationUntilAllMessagesAreConsumedAndOperationIdIsNotTheSame() throws Exception {
        AMQP.BasicProperties basicProperties = mock(AMQP.BasicProperties.class);
        when(basicProperties.getHeaders()).thenReturn(new HashMap<>());
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.getEnvelope()).thenReturn(DEFAULT_ENVELOPE);
        when(getResponse.getProps()).thenReturn(basicProperties);

        testSuccessfulMassOperationForMessage(getResponse);
    }

    private void testSuccessfulMassOperationForMessage(GetResponse getResponse) throws Exception {
        when(channel.basicGet(DEFAULT_QUEUE_NAME, false)).thenReturn(getResponse, getResponse, null);

        sut.consumeAllMessageAndApplyFunctionAndAcknowledgeOnSuccess(DEFAULT_VHOST_NAME, DEFAULT_QUEUE_NAME, basicFunction);

        InOrder io = Mockito.inOrder(channel, basicFunction);
        io.verify(channel).basicGet(DEFAULT_QUEUE_NAME, false);
        io.verify(basicFunction).apply(DEFAULT_OPERATION_ID, channel, getResponse);
        io.verify(channel).basicAck(DEFAULT_DELIVERY_TAG, false);
        io.verify(channel).basicGet(DEFAULT_QUEUE_NAME, false);
        io.verify(basicFunction).apply(DEFAULT_OPERATION_ID, channel, getResponse);
        io.verify(channel).basicAck(DEFAULT_DELIVERY_TAG, false);
        io.verify(channel).basicGet(DEFAULT_QUEUE_NAME, false);
    }

    @Test
    void shouldStopToPerformMassOperationWhenSameOperationIdAppearsInHeader() throws Exception {
        final Map<String, Object> headers = mock(Map.class);
        AMQP.BasicProperties basicProperties = mock(AMQP.BasicProperties.class);
        GetResponse getResponse = mock(GetResponse.class);

        when(basicProperties.getHeaders()).thenReturn(headers);
        when(getResponse.getEnvelope()).thenReturn(DEFAULT_ENVELOPE);
        when(getResponse.getProps()).thenReturn(basicProperties);
        when(headers.getOrDefault(eq(OperationId.HEADER_NAME), anyString())).thenReturn("otherHeader", "otherHeader",
                DEFAULT_OPERATION_ID.value());

        when(channel.basicGet(DEFAULT_QUEUE_NAME, false)).thenReturn(getResponse, getResponse, getResponse);

        sut.consumeAllMessageAndApplyFunctionAndAcknowledgeOnSuccess(DEFAULT_VHOST_NAME, DEFAULT_QUEUE_NAME, basicFunction);

        InOrder io = Mockito.inOrder(channel, basicFunction);
        io.verify(channel).basicGet(DEFAULT_QUEUE_NAME, false);
        io.verify(basicFunction).apply(DEFAULT_OPERATION_ID, channel, getResponse);
        io.verify(channel).basicAck(DEFAULT_DELIVERY_TAG, false);
        io.verify(channel).basicGet(DEFAULT_QUEUE_NAME, false);
        io.verify(basicFunction).apply(DEFAULT_OPERATION_ID, channel, getResponse);
        io.verify(channel).basicAck(DEFAULT_DELIVERY_TAG, false);
        io.verify(channel).basicGet(DEFAULT_QUEUE_NAME, false);
        io.verify(channel).basicNack(DEFAULT_DELIVERY_TAG, false, true);
    }

    @Test
    void shouldFailToRunMassOperationWhenMessagesCannotBeConsumed() throws Exception {
        IOException expectedException = new IOException();

        when(channel.basicGet(DEFAULT_QUEUE_NAME, false)).thenThrow(expectedException);

        try {
            sut.consumeAllMessageAndApplyFunctionAndAcknowledgeOnSuccess(DEFAULT_VHOST_NAME, DEFAULT_QUEUE_NAME, basicFunction);
            fail();
        } catch (MessageOperationFailedException e) {
            assertEquals(expectedException, e.getCause());
        }

        verify(channel).basicGet(DEFAULT_QUEUE_NAME, false);
        verify(channel).close();
        verifyNoMoreInteractions(channel);
    }

    @Test
    void shouldFailToRunMassOperationWhenFunctionThrowsAnException() throws Exception {
        GetResponse getResponse = mock(GetResponse.class);
        MessageOperationFailedException expectedException = Mockito.mock(MessageOperationFailedException.class);

        when(getResponse.getEnvelope()).thenReturn(DEFAULT_ENVELOPE);
        when(getResponse.getProps()).thenReturn(DEFAULT_BASIC_PROPERTIES);

        when(channel.basicGet(DEFAULT_QUEUE_NAME, false)).thenReturn(getResponse);
        doThrow(expectedException).when(basicFunction).apply(DEFAULT_OPERATION_ID, channel, getResponse);

        try {
            sut.consumeAllMessageAndApplyFunctionAndAcknowledgeOnSuccess(DEFAULT_VHOST_NAME, DEFAULT_QUEUE_NAME, basicFunction);
            fail();
        } catch (MessageOperationFailedException e) {
            assertEquals(expectedException, e);
        }

        verify(channel).basicGet(DEFAULT_QUEUE_NAME, false);
        verify(basicFunction).apply(DEFAULT_OPERATION_ID, channel, getResponse);
        verify(channel).basicNack(DEFAULT_DELIVERY_TAG, false, true);
        verify(channel).close();
        verifyNoMoreInteractions(channel);
    }

    @Test
    void shouldRouteFirstMessage() throws Exception {
        GetResponse getResponse = mockDefaultGetResponse();
        RoutingDetails routingDetails = mockDefaultRoutingDetails();
        AMQP.BasicProperties mappedBasicProperties = mock(AMQP.BasicProperties.class);

        when(messageChecksum.createFor(DEFAULT_BASIC_PROPERTIES, DEFAULT_PAYLOAD)).thenReturn(DEFAULT_CHECKSUM);
        when(stateKeepingReturnListenerFactory.createFor(eq(DEFAULT_OPERATION_ID), any(Logger.class))).thenReturn(
                stateKeepingReturnListener);
        when(routeResolvingFunction.resolve(getResponse)).thenReturn(routingDetails);
        when(channel.basicGet(DEFAULT_QUEUE_NAME, false)).thenReturn(getResponse);
        when(routingMessageHeaderModifier.modifyHeaders(DEFAULT_BASIC_PROPERTIES, DEFAULT_OPERATION_ID,
                DEFAULT_ROUTING_COUNT_HEADER)).thenReturn(mappedBasicProperties);

        sut.routeFirstMessageAndAcknowledgeOnSuccess(DEFAULT_VHOST_NAME, DEFAULT_QUEUE_NAME, DEFAULT_CHECKSUM,
                routeResolvingFunction);

        InOrder io = Mockito.inOrder(channel, routeResolvingFunction, routingMessageHeaderModifier);
        verifyMessageRouted(getResponse, mappedBasicProperties, io);
        io.verify(channel).close();
        io.verifyNoMoreInteractions();
    }

    @Test
    void shouldFailToRouteFirstMessageWhenReturnListenerWasTriggered() throws Exception {
        GetResponse getResponse = mockDefaultGetResponse();
        RoutingDetails routingDetails = mockDefaultRoutingDetails();
        AMQP.BasicProperties mappedBasicProperties = mock(AMQP.BasicProperties.class);

        when(messageChecksum.createFor(DEFAULT_BASIC_PROPERTIES, DEFAULT_PAYLOAD)).thenReturn(DEFAULT_CHECKSUM);
        when(stateKeepingReturnListenerFactory.createFor(eq(DEFAULT_OPERATION_ID), any(Logger.class))).thenReturn(
                stateKeepingReturnListener);
        when(routeResolvingFunction.resolve(getResponse)).thenReturn(routingDetails);
        when(channel.basicGet(DEFAULT_QUEUE_NAME, false)).thenReturn(getResponse);
        when(routingMessageHeaderModifier.modifyHeaders(DEFAULT_BASIC_PROPERTIES, DEFAULT_OPERATION_ID,
                DEFAULT_ROUTING_COUNT_HEADER)).thenReturn(mappedBasicProperties);
        when(stateKeepingReturnListener.isReceived()).thenReturn(true);

        try {
            sut.routeFirstMessageAndAcknowledgeOnSuccess(DEFAULT_VHOST_NAME, DEFAULT_QUEUE_NAME, DEFAULT_CHECKSUM,
                    routeResolvingFunction);
            fail();
        } catch (MessageOperationFailedException e) {
            assertThat(e.getMessage(), containsString("basic.return received"));
        }

        InOrder io = Mockito.inOrder(channel, routeResolvingFunction, routingMessageHeaderModifier);
        io.verify(channel).basicGet(DEFAULT_QUEUE_NAME, false);
        io.verify(routeResolvingFunction).resolve(getResponse);
        io.verify(channel).addReturnListener(stateKeepingReturnListener);
        io.verify(channel).confirmSelect();
        io.verify(routingMessageHeaderModifier)
                .modifyHeaders(DEFAULT_BASIC_PROPERTIES, DEFAULT_OPERATION_ID, DEFAULT_ROUTING_COUNT_HEADER);
        io.verify(channel)
                .basicPublish(DEFAULT_ROUTING_TARGET_EXCHANGE, DEFAULT_ROUTING_TARGET_ROUTING_KEY, true, mappedBasicProperties,
                        DEFAULT_PAYLOAD);
        io.verify(channel).waitForConfirmsOrDie(MessageOperationExecutor.MAX_WAIT_FOR_CONFIRM);
        io.verify(channel).basicNack(DEFAULT_DELIVERY_TAG, false, true);
        io.verify(channel).close();
        io.verifyNoMoreInteractions();
    }

    @Test
    void shouldRouteAllMessage() throws Exception {
        GetResponse getResponse = mockDefaultGetResponse();
        RoutingDetails routingDetails = mockDefaultRoutingDetails();
        AMQP.BasicProperties mappedBasicProperties = mock(AMQP.BasicProperties.class);

        when(stateKeepingReturnListenerFactory.createFor(eq(DEFAULT_OPERATION_ID), any(Logger.class))).thenReturn(
                stateKeepingReturnListener);
        when(routeResolvingFunction.resolve(getResponse)).thenReturn(routingDetails);
        when(channel.basicGet(DEFAULT_QUEUE_NAME, false)).thenReturn(getResponse, getResponse, null);
        when(routingMessageHeaderModifier.modifyHeaders(DEFAULT_BASIC_PROPERTIES, DEFAULT_OPERATION_ID,
                DEFAULT_ROUTING_COUNT_HEADER)).thenReturn(mappedBasicProperties);

        sut.routeAllMessagesAndAcknowledgeOnSuccess(DEFAULT_VHOST_NAME, DEFAULT_QUEUE_NAME, routeResolvingFunction);

        InOrder io = Mockito.inOrder(channel, routeResolvingFunction, routingMessageHeaderModifier);
        verifyMessageRouted(getResponse, mappedBasicProperties, io);
        verifyMessageRouted(getResponse, mappedBasicProperties, io);
        io.verify(channel).basicGet(DEFAULT_QUEUE_NAME, false);
        io.verify(channel).close();
        io.verifyNoMoreInteractions();
    }

    @Test
    void shouldRouteSelectedMessages() throws Exception {
        GetResponse getResponse = mockDefaultGetResponse();
        GetResponse alreadyProcessedResponse = mockGetResponseWithDefaultOperationId();

        RoutingDetails routingDetails = mockDefaultRoutingDetails();
        AMQP.BasicProperties mappedBasicProperties = mock(AMQP.BasicProperties.class);
        String selectedMessageChecksum = UUID.randomUUID().toString();

        when(DEFAULT_ENVELOPE.getExchange()).thenReturn(DEFAULT_ROUTING_TARGET_EXCHANGE);
        when(DEFAULT_ENVELOPE.getRoutingKey()).thenReturn(DEFAULT_ROUTING_TARGET_ROUTING_KEY);
        when(messageChecksum.createFor(DEFAULT_BASIC_PROPERTIES, DEFAULT_PAYLOAD))
                .thenReturn(DEFAULT_CHECKSUM, selectedMessageChecksum, DEFAULT_CHECKSUM, selectedMessageChecksum);
        when(stateKeepingReturnListenerFactory.createFor(eq(DEFAULT_OPERATION_ID), any(Logger.class)))
                .thenReturn(stateKeepingReturnListener);
        when(routeResolvingFunction.resolve(getResponse)).thenReturn(routingDetails);
        when(channel.basicGet(DEFAULT_QUEUE_NAME, false))
                .thenReturn(getResponse, getResponse, getResponse, getResponse, alreadyProcessedResponse);
        when(routingMessageHeaderModifier.modifyHeaders(DEFAULT_BASIC_PROPERTIES, DEFAULT_OPERATION_ID,
                DEFAULT_ROUTING_COUNT_HEADER)).thenReturn(mappedBasicProperties);

        sut.routeMessagesAndAcknowledgeOnSuccess(DEFAULT_VHOST_NAME, DEFAULT_QUEUE_NAME, List.of(selectedMessageChecksum),
                routeResolvingFunction);

        InOrder io = Mockito.inOrder(channel, routeResolvingFunction, routingMessageHeaderModifier);
        verifyMessageRouted(getResponse, mappedBasicProperties, io);
        verifyMessageReinserted(getResponse, mappedBasicProperties, io);
        verifyMessageRouted(getResponse, mappedBasicProperties, io);
        verifyMessageReinserted(getResponse, mappedBasicProperties, io);
        io.verify(channel).basicGet(DEFAULT_QUEUE_NAME, false);
        io.verify(channel).basicNack(DEFAULT_DELIVERY_TAG, false, true);
        io.verify(channel).close();
        io.verifyNoMoreInteractions();
    }

    @Test
    void shouldFailToRouteAllMessageWhenReturnListenerWasTriggered() throws Exception {
        GetResponse getResponse = mockDefaultGetResponse();
        RoutingDetails routingDetails = mockDefaultRoutingDetails();
        AMQP.BasicProperties mappedBasicProperties = mock(AMQP.BasicProperties.class);

        when(stateKeepingReturnListenerFactory.createFor(eq(DEFAULT_OPERATION_ID), any(Logger.class))).thenReturn(
                stateKeepingReturnListener);
        when(routeResolvingFunction.resolve(getResponse)).thenReturn(routingDetails);
        when(channel.basicGet(DEFAULT_QUEUE_NAME, false)).thenReturn(getResponse, getResponse, null);
        when(routingMessageHeaderModifier.modifyHeaders(DEFAULT_BASIC_PROPERTIES, DEFAULT_OPERATION_ID,
                DEFAULT_ROUTING_COUNT_HEADER)).thenReturn(mappedBasicProperties);
        when(stateKeepingReturnListener.isReceived()).thenReturn(false, true);

        try {
            sut.routeAllMessagesAndAcknowledgeOnSuccess(DEFAULT_VHOST_NAME, DEFAULT_QUEUE_NAME, routeResolvingFunction);
            fail();
        } catch (MessageOperationFailedException e) {
            assertThat(e.getMessage(), containsString("basic.return received"));
        }

        InOrder io = Mockito.inOrder(channel, routeResolvingFunction, routingMessageHeaderModifier);
        verifyMessageRouted(getResponse, mappedBasicProperties, io);
        io.verify(channel).basicGet(DEFAULT_QUEUE_NAME, false);
        io.verify(routeResolvingFunction).resolve(getResponse);
        io.verify(channel).addReturnListener(stateKeepingReturnListener);
        io.verify(channel).confirmSelect();
        io.verify(routingMessageHeaderModifier)
                .modifyHeaders(DEFAULT_BASIC_PROPERTIES, DEFAULT_OPERATION_ID, DEFAULT_ROUTING_COUNT_HEADER);
        io.verify(channel)
                .basicPublish(DEFAULT_ROUTING_TARGET_EXCHANGE, DEFAULT_ROUTING_TARGET_ROUTING_KEY, true, mappedBasicProperties,
                        DEFAULT_PAYLOAD);
        io.verify(channel).waitForConfirmsOrDie(MessageOperationExecutor.MAX_WAIT_FOR_CONFIRM);
        io.verify(channel).basicNack(DEFAULT_DELIVERY_TAG, false, true);
        io.verify(channel).close();
        io.verifyNoMoreInteractions();
    }

    private void verifyMessageRouted(GetResponse getResponse, AMQP.BasicProperties mappedBasicProperties, InOrder io)
            throws IOException, InterruptedException,
            TimeoutException {
        io.verify(channel).basicGet(DEFAULT_QUEUE_NAME, false);
        io.verify(routeResolvingFunction).resolve(getResponse);
        io.verify(channel).addReturnListener(stateKeepingReturnListener);
        io.verify(channel).confirmSelect();
        io.verify(routingMessageHeaderModifier)
                .modifyHeaders(DEFAULT_BASIC_PROPERTIES, DEFAULT_OPERATION_ID, DEFAULT_ROUTING_COUNT_HEADER);
        io.verify(channel)
                .basicPublish(DEFAULT_ROUTING_TARGET_EXCHANGE, DEFAULT_ROUTING_TARGET_ROUTING_KEY, true, mappedBasicProperties,
                        DEFAULT_PAYLOAD);
        io.verify(channel).waitForConfirmsOrDie(MessageOperationExecutor.MAX_WAIT_FOR_CONFIRM);
        io.verify(channel).removeReturnListener(stateKeepingReturnListener);
        io.verify(channel).basicAck(DEFAULT_DELIVERY_TAG, false);
    }

    private void verifyMessageReinserted(GetResponse getResponse, AMQP.BasicProperties mappedBasicProperties, InOrder io)
            throws IOException, InterruptedException,
            TimeoutException {
        io.verify(channel).basicGet(DEFAULT_QUEUE_NAME, false);
        io.verify(routeResolvingFunction).resolve(getResponse);
        io.verify(channel).addReturnListener(stateKeepingReturnListener);
        io.verify(channel).confirmSelect();
        io.verify(routingMessageHeaderModifier)
                .modifyHeaders(DEFAULT_BASIC_PROPERTIES, DEFAULT_OPERATION_ID, DEFAULT_ROUTING_COUNT_HEADER);
        io.verify(channel)
                .basicPublish(DEFAULT_ROUTING_TARGET_EXCHANGE, DEFAULT_ROUTING_TARGET_ROUTING_KEY, true, mappedBasicProperties,
                        DEFAULT_PAYLOAD);
        io.verify(channel).waitForConfirmsOrDie(MessageOperationExecutor.MAX_WAIT_FOR_CONFIRM);
        io.verify(channel).removeReturnListener(stateKeepingReturnListener);
        io.verify(channel).basicAck(DEFAULT_DELIVERY_TAG, false);
    }

    private RoutingDetails mockDefaultRoutingDetails() {
        RoutingDetails routingDetails = mock(RoutingDetails.class);
        when(routingDetails.countHeaderName()).thenReturn(DEFAULT_ROUTING_COUNT_HEADER);
        when(routingDetails.exchange()).thenReturn(DEFAULT_ROUTING_TARGET_EXCHANGE);
        when(routingDetails.routingKey()).thenReturn(DEFAULT_ROUTING_TARGET_ROUTING_KEY);
        return routingDetails;
    }

    private GetResponse mockDefaultGetResponse() {
        GetResponse response = mock(GetResponse.class);
        when(response.getEnvelope()).thenReturn(DEFAULT_ENVELOPE);
        when(response.getProps()).thenReturn(DEFAULT_BASIC_PROPERTIES);
        when(response.getBody()).thenReturn(DEFAULT_PAYLOAD);
        return response;
    }

    private GetResponse mockGetResponseWithDefaultOperationId() {
        final var basicProperties = mock(AMQP.BasicProperties.class);
        when(basicProperties.getHeaders()).thenReturn(Map.of(OperationId.HEADER_NAME, DEFAULT_OPERATION_ID));
        GetResponse response = mock(GetResponse.class);
        when(response.getEnvelope()).thenReturn(DEFAULT_ENVELOPE);
        when(response.getProps()).thenReturn(basicProperties);
        when(response.getBody()).thenReturn(DEFAULT_PAYLOAD);
        return response;
    }

}
