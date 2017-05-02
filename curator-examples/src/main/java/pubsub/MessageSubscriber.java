package pubsub;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.x.async.modeled.cached.CachedModeledFramework;
import org.apache.curator.x.async.modeled.cached.ModeledCacheListener;
import pubsub.builders.Clients;
import pubsub.models.Group;
import pubsub.models.Message;
import pubsub.models.Priority;
import java.io.Closeable;

public class MessageSubscriber implements Closeable
{
    private final CachedModeledFramework<Message> client;

    public MessageSubscriber(CuratorFramework client, Group group, Priority priority)
    {
        this.client = Clients.messageClient.resolved(client, group, priority).cached();
    }

    public Listenable<ModeledCacheListener<Message>> listenable()
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
