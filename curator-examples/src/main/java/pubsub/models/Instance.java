package pubsub.models;

import org.apache.curator.x.async.modeled.NodeName;
import java.util.Objects;
import java.util.UUID;

public class Instance implements NodeName
{
    private final String id;
    private final InstanceType type;
    private final String hostname;
    private final int port;

    public Instance()
    {
        this(UUID.randomUUID().toString(), InstanceType.proxy, "", 0);
    }

    public Instance(String id, InstanceType type, String hostname, int port)
    {
        this.id = Objects.requireNonNull(id, "id cannot be null");
        this.type = Objects.requireNonNull(type, "type cannot be null");
        this.hostname = Objects.requireNonNull(hostname, "hostname cannot be null");
        this.port = port;
    }

    public String getId()
    {
        return id;
    }

    public InstanceType getType()
    {
        return type;
    }

    public String getHostname()
    {
        return hostname;
    }

    public int getPort()
    {
        return port;
    }

    @Override
    public String nodeName()
    {
        return id;
    }

    @Override
    public String toString()
    {
        return "Instance{" + "id='" + id + '\'' + ", type=" + type + ", hostname='" + hostname + '\'' + ", port=" + port + '}';
    }
}
