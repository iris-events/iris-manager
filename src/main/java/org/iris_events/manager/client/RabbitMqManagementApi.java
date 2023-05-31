package org.iris_events.manager.client;

import java.util.List;

import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

import org.iris_events.manager.client.model.Binding;
import org.iris_events.manager.client.model.Exchange;
import org.iris_events.manager.client.model.Queue;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@RegisterRestClient(configKey = "rabbitmq.management.api")
@RegisterProvider(RabbitMqAuthProvider.class)
@Path("/api/")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public interface RabbitMqManagementApi {

    @GET
    @Path("/exchanges/{vhost}")
    List<Exchange> getExchanges(@PathParam("vhost") String vhost);

    @GET
    @Path("/exchanges/{vhost}/{exchange}/bindings/source")
    List<Binding> getExchangeSourceBindings(@PathParam("vhost") String vhost,
                                            @PathParam("exchange") String exchange);

    @GET
    @Path("/queues")
    List<Queue> getQueues();

    @GET
    @Path("/queues/{vhost}")
    List<Queue> getQueues(@PathParam("vhost") String vhost);

    @DELETE
    @Path("/queues/{vhost}/{queue}/contents")
    List<Queue> purgeQueue(@PathParam("vhost") String vhost, @PathParam("queue") String queue);

    @GET
    @Path("/queues/{vhost}/{queue}/bindings")
    List<Binding> getQueueBindings(@PathParam("vhost") String vhost, @PathParam("queue") String queue);
}
