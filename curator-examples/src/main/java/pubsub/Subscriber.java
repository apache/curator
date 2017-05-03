package pubsub;

import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.modeled.cached.CachedModeledFramework;
import org.apache.curator.x.async.modeled.typed.TypedModeledFramework2;
import pubsub.messages.LocationAvailable;
import pubsub.messages.UserCreated;
import pubsub.models.Group;
import pubsub.models.Instance;
import pubsub.models.InstanceType;
import pubsub.models.Message;
import pubsub.models.Priority;

import static pubsub.builders.Clients.*;

public class Subscriber
{
    private final AsyncCuratorFramework client;

    public Subscriber(AsyncCuratorFramework client)
    {
        this.client = client;
    }

    public CachedModeledFramework<LocationAvailable> startLocationAvailableSubscriber(Group group, Priority priority)
    {
        return startSubscriber(locationAvailableClient, group, priority);
    }

    public CachedModeledFramework<UserCreated> startUserCreatedSubscriber(Group group, Priority priority)
    {
        return startSubscriber(userCreatedClient, group, priority);
    }

    public CachedModeledFramework<Instance> startInstanceSubscriber(InstanceType instanceType)
    {
        CachedModeledFramework<Instance> resolved = instanceClient.resolved(client, instanceType).cached();
        resolved.start();
        return resolved;
    }

    public <T extends Message> CachedModeledFramework<T> startSubscriber(TypedModeledFramework2<T, Group, Priority> typedClient, Group group, Priority priority)
    {
        CachedModeledFramework<T> resolved = typedClient.resolved(client, group, priority).cached();
        resolved.start();
        return resolved;
    }
}
