package id.global.iris.manager.queue;

import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.rest.client.inject.RestClient;

import id.global.iris.manager.client.RabbitMqManagementApi;
import id.global.iris.manager.client.model.Binding;
import id.global.iris.manager.client.model.Exchange;
import id.global.iris.manager.client.model.Queue;
import id.global.iris.manager.queue.model.Message;
import id.global.iris.manager.queue.operations.Operations;

@ApplicationScoped
public class RabbitMqFacade {

    @Inject
    @RestClient
    RabbitMqManagementApi managementApi;

    @Inject
    Operations operations;

    public List<Exchange> getExchanges(String vhost) {
        return managementApi.getExchanges(vhost);
    }

    public List<Queue> getQueues() {
        return managementApi.getQueues();
    }

    public List<Queue> getQueues(String vhost) {
        return managementApi.getQueues(vhost);
    }

    public List<Binding> getExchangeSourceBindings(String vhost, String exchange) {
        return managementApi.getExchangeSourceBindings(vhost, exchange);
    }

    public List<Binding> getQueueBindings(String vhost, String queueName) {
        return managementApi.getQueueBindings(vhost, queueName);
    }

    public List<Message> getMessagesOfQueue(String vhost, String queueName, int limit) {
        return operations.getMessagesOfQueue(vhost, queueName, limit);
    }

    public void purgeQueue(String vhost, String queueName) {
        managementApi.purgeQueue(vhost, queueName);
    }

    public void deleteFirstMessageInQueue(String vhost, String queueName, String messageChecksum) {
        operations.deleteFirstMessageInQueue(vhost, queueName, messageChecksum);
    }

    public void deleteMultipleMessageInQueue(String vhost, String queueName, List<String> checksums) {
        operations.deleteMultipleMessagesInQueue(vhost, queueName, checksums);
    }

    public void moveAllMessagesInQueue(String vhost, String queueName, String targetExchange, String targetRoutingKey) {
        operations.moveAllMessagesInQueue(vhost, queueName, targetExchange, targetRoutingKey);
    }

    public void moveMultipleMessagesInQueue(String vhost, String queueName, List<String> checksums, String targetExchange,
            String targetRoutingKey) {
        operations.moveMultipleMessagesInQueue(vhost, queueName, checksums, targetExchange, targetRoutingKey);
    }

    public void moveFirstMessageInQueue(String vhost, String queueName, String messageChecksum, String targetExchange,
            String targetRoutingKey) {
        operations.moveFirstMessageInQueue(vhost, queueName, messageChecksum, targetExchange, targetRoutingKey);
    }

    public void requeueAllMessagesInQueue(String vhost, String queue) {
        operations.requeueAllMessagesInQueue(vhost, queue);
    }

    public void requeueMultipleMessagesInQueue(String vhost, String queue, List<String> checksums) {
        operations.requeueMultipleMessagesInQueue(vhost, queue, checksums);
    }

    public void requeueFirstMessageInQueue(String vhost, String queue, String checksum) {
        operations.requeueFirstMessageInQueue(vhost, queue, checksum);
    }
}
