package pubsub;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.modeled.ModeledFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pubsub.models.Group;
import pubsub.models.Instance;
import pubsub.models.Message;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static pubsub.builders.Clients.instanceClient;
import static pubsub.builders.Clients.messageClient;

public class Publisher
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final AsyncCuratorFramework client;

    public Publisher(CuratorFramework client)
    {
        this.client = AsyncCuratorFramework.wrap(Objects.requireNonNull(client, "client cannot be null"));
    }

    public void publishInstance(Instance instance)
    {
        ModeledFramework<Instance> resolvedClient = instanceClient
            .resolved(client.unwrap(), instance.getType())
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
                .resolved(client.unwrap(), instance.getType())
                .resolved(instance)
                .createOp(instance)
            )
            .collect(Collectors.toList());
        client.transaction().forOperations(operations).exceptionally(e -> {
            log.error("Could not publish instances: " + instances, e);
            return null;
        });
    }

    public void publishMessage(Message message, Group group)
    {
        ModeledFramework<Message> resolvedClient = messageClient
            .resolved(client.unwrap(), group, message.getPriority())
            .resolved(message);
        resolvedClient.set(message).exceptionally(e -> {
            log.error("Could not publish message: " + message, e);
            return null;
        });
    }

    public void publishMessages(List<Message> messages, Group group)
    {
        List<CuratorOp> operations = messages.stream()
            .map(message -> messageClient
                    .resolved(client.unwrap(), group, message.getPriority())
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
