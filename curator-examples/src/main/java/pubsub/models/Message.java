package pubsub.models;

import org.apache.curator.x.async.modeled.NodeName;
import java.util.Objects;
import java.util.UUID;

public abstract class Message implements NodeName
{
    private final String id;
    private final Priority priority;

    protected Message()
    {
        this(UUID.randomUUID().toString(), Priority.low);
    }

    protected Message(Priority priority)
    {
        this(UUID.randomUUID().toString(), priority);
    }

    protected Message(String id, Priority priority)
    {
        this.id = Objects.requireNonNull(id, "id cannot be null");
        this.priority = Objects.requireNonNull(priority, "messageType cannot be null");
    }

    public String getId()
    {
        return id;
    }

    public Priority getPriority()
    {
        return priority;
    }

    @Override
    public String nodeName()
    {
        return id;
    }

    @Override
    public String toString()
    {
        return "Message{" + "id='" + id + '\'' + ", priority=" + priority + '}';
    }
}
