package id.global.iris.manager.controller;

import java.util.List;

import javax.annotation.security.RolesAllowed;
import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import id.global.iris.manager.queue.RabbitMqFacade;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Path("/v1/iris-manager/queues")
@RequestScoped
@RolesAllowed({ "admin.iris" })
public class QueueManagerResource {

    private static final Logger log = LoggerFactory.getLogger(QueueManagerResource.class);
    private static final String DEFAULT_LIMIT = "10";
    private static final String DEFAULT_VHOST = "/";

    private final RabbitMqFacade facade;

    @Inject
    public QueueManagerResource(RabbitMqFacade facade) {
        this.facade = facade;
    }

    @GET
    public Response getQueues() {
        final var queues = facade.getQueues();
        return Response.ok(queues).build();
    }

    @GET
    @Path("/{queue}")
    public Response getMessages(
            @PathParam(Parameters.QUEUE) String queue,
            @DefaultValue(DEFAULT_VHOST) @QueryParam(Parameters.VHOST) String vhost,
            @DefaultValue(DEFAULT_LIMIT) @QueryParam(Parameters.LIMIT) Integer limit) {

        final var messagesOfQueue = facade.getMessagesOfQueue(vhost, queue, limit);
        return Response.ok(messagesOfQueue).build();
    }

    @POST
    @Path("/{queue}/requeue-first")
    public Response requeueFirstMessage(
            @PathParam(Parameters.QUEUE) String queue,
            @DefaultValue(DEFAULT_VHOST) @QueryParam(Parameters.VHOST) String vhost,
            @QueryParam(Parameters.CHECKSUM) String checksum) {
        try {
            facade.requeueFirstMessageInQueue(vhost, queue, checksum);
            return Response.accepted().build();
        } catch (Exception e) {
            log.error(
                    "Failed to requeue first message with checksum {} of queue {} of vhost {}", checksum, queue, vhost, e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path("/{queue}/requeue")
    public Response requeueMultipleMessages(
            @PathParam(Parameters.QUEUE) String queue,
            @DefaultValue(DEFAULT_VHOST) @QueryParam(Parameters.VHOST) String vhost,
            List<String> checksums) {
        try {
            facade.requeueMultipleMessagesInQueue(vhost, queue, checksums);
            return Response.accepted().build();
        } catch (Exception e) {
            log.error("Failed to requeue all messages from queue {} of vhost {}", queue, vhost, e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path("/{queue}/requeue-all")
    public Response requeueAllMessages(
            @PathParam(Parameters.QUEUE) String queue,
            @DefaultValue(DEFAULT_VHOST) @QueryParam(Parameters.VHOST) String vhost) {
        try {
            facade.requeueAllMessagesInQueue(vhost, queue);
            return Response.accepted().build();
        } catch (Exception e) {
            log.error("Failed to requeue all messages from queue {} of vhost {}", queue, vhost, e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path("/{queue}/move-first")
    public Response moveFirstMessage(
            @PathParam(Parameters.QUEUE) String queue,
            @DefaultValue(DEFAULT_VHOST) @QueryParam(Parameters.VHOST) String vhost,
            @QueryParam(Parameters.CHECKSUM) String checksum,
            @QueryParam(Parameters.TARGET_EXCHANGE) String targetExchange,
            @QueryParam(Parameters.TARGET_ROUTING_KEY) String targetRoutingKey) {

        try {
            facade.moveFirstMessageInQueue(vhost, queue, checksum, targetExchange, targetRoutingKey);
            return Response.accepted().build();
        } catch (Exception e) {
            log.error(
                    "Failed to move message with checksum {} from queue {} of vhost {} to target exchange {} and routing key {}",
                    checksum, queue, vhost, targetExchange, targetRoutingKey, e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path("/{queue}/move")
    public Response moveMultipleMessages(
            @PathParam(Parameters.QUEUE) String queue,
            @DefaultValue(DEFAULT_VHOST) @QueryParam(Parameters.VHOST) String vhost,
            @QueryParam(Parameters.TARGET_EXCHANGE) String targetExchange,
            @QueryParam(Parameters.TARGET_ROUTING_KEY) String targetRoutingKey,
            List<String> checksums) {

        try {
            facade.moveMultipleMessagesInQueue(vhost, queue, checksums, targetExchange, targetRoutingKey);
            return Response.accepted().build();
        } catch (Exception e) {
            log.error("Failed to move all messages from queue {} of vhost {} to exchange {} with routing key {}", queue, vhost,
                    targetExchange, targetRoutingKey, e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path("/{queue}/move-all")
    public Response moveAllMessages(
            @PathParam(Parameters.QUEUE) String queue,
            @DefaultValue(DEFAULT_VHOST) @QueryParam(Parameters.VHOST) String vhost,
            @QueryParam(Parameters.TARGET_EXCHANGE) String targetExchange,
            @QueryParam(Parameters.TARGET_ROUTING_KEY) String targetRoutingKey) {

        try {
            facade.moveAllMessagesInQueue(vhost, queue, targetExchange, targetRoutingKey);
            return Response.accepted().build();
        } catch (Exception e) {
            log.error("Failed to move all messages from queue {} of vhost {} to exchange {} with routing key {}", queue, vhost,
                    targetExchange, targetRoutingKey, e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path("/{queue}/delete-first")
    public Response deleteFirstMessage(
            @PathParam(Parameters.QUEUE) String queue,
            @DefaultValue(DEFAULT_VHOST) @QueryParam(Parameters.VHOST) String vhost,
            @QueryParam(Parameters.CHECKSUM) String checksum) {
        try {
            facade.deleteFirstMessageInQueue(vhost, queue, checksum);
            return Response.accepted().build();
        } catch (Exception e) {
            log.error("Failed to delete first message with checksum {} from queue {} of vhost {}", checksum, queue, vhost, e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path("/{queue}/delete")
    public Response deleteAllMessages(
            @PathParam(Parameters.QUEUE) String queue,
            @DefaultValue(DEFAULT_VHOST) @QueryParam(Parameters.VHOST) String vhost,
            List<String> checksums) {
        try {
            facade.deleteMultipleMessageInQueue(vhost, queue, checksums);
            return Response.accepted().build();
        } catch (Exception e) {
            log.error("Failed to delete all messages or queue {} of vhost {}", queue, vhost, e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path("/{queue}/delete-all")
    public Response deleteAllMessages(
            @PathParam(Parameters.QUEUE) String queue,
            @DefaultValue(DEFAULT_VHOST) @QueryParam(Parameters.VHOST) String vhost) {
        try {
            facade.purgeQueue(vhost, queue);
            return Response.accepted().build();
        } catch (Exception e) {
            log.error("Failed to delete all messages or queue {} of vhost {}", queue, vhost, e);
            return Response.serverError().build();
        }
    }
}
