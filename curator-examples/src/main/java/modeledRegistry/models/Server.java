package modeledRegistry.models;

import java.util.Objects;

public abstract class Server
{
    private final String id;
    private final String ipAddress;

    protected Server(String id, String ipAddress)
    {
        this.id = Objects.requireNonNull(id, "id cannot be null");
        this.ipAddress = Objects.requireNonNull(ipAddress, "ipAddress cannot be null");
    }

    public String getId()
    {
        return id;
    }

    public String getIpAddress()
    {
        return ipAddress;
    }
}
