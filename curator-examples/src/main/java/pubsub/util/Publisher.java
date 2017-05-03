package pubsub.util;

import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.modeled.ModeledFramework;
import org.apache.curator.x.async.modeled.typed.TypedModeledFramework2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pubsub.messages.LocationAvailable;
import pubsub.messages.UserCreated;
import pubsub.models.Group;
import pubsub.models.Instance;
import pubsub.models.Message;
import pubsub.models.Priority;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static pubsub.builders.Clients.*;

public class Publisher
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final AsyncCuratorFramework client;

    public Publisher(AsyncCuratorFramework client)
    {
        this.client = Objects.requireNonNull(client, "client cannot be null");
    }

    /**
     * Publish the given instance using the Instance client template
     *
     * @param instance instance to publish
     */
    public void publishInstance(Instance instance)
    {
        ModeledFramework<Instance> resolvedClient = instanceClient
            .resolved(client, instance.getType())   // this resolves to the parent path
            .resolved(instance);                    // this resolves to a child node - uses the Instance's id because Instance extends NodeName
        resolvedClient.set(instance).exceptionally(e -> {
            log.error("Could not publish instance: " + instance, e);
            return null;
        });
    }

    /**
     * Publish the given instances using the Instance client template in a transaction
     *
     * @param instances instances to publish
     */
    public void publishInstances(List<Instance> instances)
    {
        List<CuratorOp> operations = instances.stream()
            .map(instance -> instanceClient
                .resolved(client, instance.getType())   // this resolves to the parent path
                .resolved(instance)                     // this resolves to a child node - uses the Instance's id because Instance extends NodeName
                .createOp(instance)
            )
            .collect(Collectors.toList());
        client.transaction().forOperations(operations).exceptionally(e -> {
            log.error("Could not publish instances: " + instances, e);
            return null;
        });
    }

    /**
     * Publish the given LocationAvailable using the LocationAvailable client template
     *
     * @param group group
     * @param locationAvailable message to publish
     */
    public void publishLocationAvailable(Group group, LocationAvailable locationAvailable)
    {
        publishMessage(locationAvailableClient, group, locationAvailable);
    }

    /**
     * Publish the given UserCreated using the UserCreated client template
     *
     * @param group group
     * @param userCreated message to publish
     */
    public void publishUserCreated(Group group, UserCreated userCreated)
    {
        publishMessage(userCreatedClient, group, userCreated);
    }

    /**
     * Publish the given LocationAvailables using the LocationAvailable client template in a transaction
     *
     * @param group group
     * @param locationsAvailable messages to publish
     */
    public void publishLocationsAvailable(Group group, List<LocationAvailable> locationsAvailable)
    {
        publishMessages(locationAvailableClient, group, locationsAvailable);
    }

    /**
     * Publish the given UserCreateds using the UserCreated client template in a transaction
     *
     * @param group group
     * @param usersCreated messages to publish
     */
    public void publishUsersCreated(Group group, List<UserCreated> usersCreated)
    {
        publishMessages(userCreatedClient, group, usersCreated);
    }

    private <T extends Message> void publishMessage(TypedModeledFramework2<T, Group, Priority> typedClient, Group group, T message)
    {
        ModeledFramework<T> resolvedClient = typedClient
            .resolved(client, group, message.getPriority())
            .resolved(message);
        resolvedClient.set(message).exceptionally(e -> {
            log.error("Could not publish message: " + message, e);
            return null;
        });
    }

    private <T extends Message> void publishMessages(TypedModeledFramework2<T, Group, Priority> typedClient, Group group, List<T> messages)
    {
        List<CuratorOp> operations = messages.stream()
            .map(message -> typedClient
                    .resolved(client, group, message.getPriority()) // this resolves to the parent path
                    .resolved(message)                              // this resolves to a child node - uses the Message's id because Message extends NodeName
                    .createOp(message)
                )
            .collect(Collectors.toList());
        client.transaction().forOperations(operations).exceptionally(e -> {
            log.error("Could not publish messages: " + messages, e);
            return null;
        });
    }
}
