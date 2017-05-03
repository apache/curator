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

    /**
     * Start a subscriber (a CachedModeledFramework instance) using the LocationAvailable client template
     *
     * @param group group to listen for
     * @param priority priority to listen for
     * @return CachedModeledFramework instance (already started)
     */
    public CachedModeledFramework<LocationAvailable> startLocationAvailableSubscriber(Group group, Priority priority)
    {
        return startSubscriber(locationAvailableClient, group, priority);
    }

    /**
     * Start a subscriber (a CachedModeledFramework instance) using the UserCreated client template
     *
     * @param group group to listen for
     * @param priority priority to listen for
     * @return CachedModeledFramework instance (already started)
     */
    public CachedModeledFramework<UserCreated> startUserCreatedSubscriber(Group group, Priority priority)
    {
        return startSubscriber(userCreatedClient, group, priority);
    }

    /**
     * Start a subscriber (a CachedModeledFramework instance) using the Instance client template
     *
     * @param instanceType type to listen for
     * @return CachedModeledFramework instance (already started)
     */
    public CachedModeledFramework<Instance> startInstanceSubscriber(InstanceType instanceType)
    {
        CachedModeledFramework<Instance> resolved = instanceClient
            .resolved(client, instanceType) // resolves to the parent path - models are children of this path
            .cached();                      // makes a cached modeled instance
        resolved.start();
        return resolved;
    }

    private <T extends Message> CachedModeledFramework<T> startSubscriber(TypedModeledFramework2<T, Group, Priority> typedClient, Group group, Priority priority)
    {
        CachedModeledFramework<T> resolved = typedClient
            .resolved(client, group, priority)  // resolves to the parent path - models are children of this path
            .cached();                          // makes a cached modeled instance
        resolved.start();
        return resolved;
    }
}
