package pubsub;

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

    public void publishInstance(Instance instance)
    {
        ModeledFramework<Instance> resolvedClient = instanceClient
            .resolved(client, instance.getType())
            .resolved(instance);
        resolvedClient.set(instance).exceptionally(e -> {
            log.error("Could not publish instance: " + instance, e);
            return null;
        });
    }

    public void publishInstances(List<Instance> instances)
    {
        List<CuratorOp> operations = instances.stream()
            .map(instance -> instanceClient
                .resolved(client, instance.getType())
                .resolved(instance)
                .createOp(instance)
            )
            .collect(Collectors.toList());
        client.transaction().forOperations(operations).exceptionally(e -> {
            log.error("Could not publish instances: " + instances, e);
            return null;
        });
    }

    public void publishLocationAvailable(Group group, LocationAvailable message)
    {
        publishMessage(locationAvailableClient, group, message);
    }

    public void publishUserCreated(Group group, UserCreated message)
    {
        publishMessage(userCreatedClient, group, message);
    }

    public void publishLocationsAvailable(Group group, List<LocationAvailable> messages)
    {
        publishMessages(locationAvailableClient, group, messages);
    }

    public void publishUsersCreated(Group group, List<UserCreated> messages)
    {
        publishMessages(userCreatedClient, group, messages);
    }

    public <T extends Message> void publishMessage(TypedModeledFramework2<T, Group, Priority> typedClient, Group group, T message)
    {
        ModeledFramework<T> resolvedClient = typedClient
            .resolved(client, group, message.getPriority())
            .resolved(message);
        resolvedClient.set(message).exceptionally(e -> {
            log.error("Could not publish message: " + message, e);
            return null;
        });
    }

    public <T extends Message> void publishMessages(TypedModeledFramework2<T, Group, Priority> typedClient, Group group, List<T> messages)
    {
        List<CuratorOp> operations = messages.stream()
            .map(message -> typedClient
                    .resolved(client, group, message.getPriority())
                    .resolved(message)
                    .createOp(message)
                )
            .collect(Collectors.toList());
        client.transaction().forOperations(operations).exceptionally(e -> {
            log.error("Could not publish messages: " + messages, e);
            return null;
        });
    }
}
