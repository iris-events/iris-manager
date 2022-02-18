package id.global.iris.manager.client;

import java.util.List;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

import id.global.iris.manager.client.model.Binding;
import id.global.iris.manager.client.model.Exchange;
import id.global.iris.manager.client.model.Queue;

@RegisterRestClient(configKey = "rabbitmq.management.api")
@RegisterProvider(RabbitMqAuthProvider.class)
@Path("/api/")
@Consumes("application/json")
@Produces("application/json")
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
