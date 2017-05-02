package pubsub;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.x.async.modeled.cached.CachedModeledFramework;
import org.apache.curator.x.async.modeled.cached.ModeledCacheListener;
import pubsub.builders.Clients;
import pubsub.models.Instance;
import pubsub.models.InstanceType;
import java.io.Closeable;

public class InstanceSubscriber implements Closeable
{
    private final CachedModeledFramework<Instance> client;

    public InstanceSubscriber(CuratorFramework client, InstanceType instanceType)
    {
        this.client = Clients.instanceClient.resolved(client, instanceType).cached();
    }

    public Listenable<ModeledCacheListener<Instance>> listenable()
    {
        return client.getCache().listenable();
    }

    public void start()
    {
        client.start();
    }

    @Override
    public void close()
    {
        client.close();
    }
}
