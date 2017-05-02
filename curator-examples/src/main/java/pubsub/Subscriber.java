package pubsub;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.async.modeled.cached.CachedModeledFramework;
import pubsub.messages.LocationAvailable;

public class Subscriber
{
    private final CuratorFramework client;

    public Subscriber(CuratorFramework client)
    {
        this.client = client;
    }
}
