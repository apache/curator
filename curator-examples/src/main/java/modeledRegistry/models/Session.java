package modeledRegistry.models;

import java.util.UUID;

public class Session extends Server
{
    private final long ttl;

    public Session()
    {
        this(UUID.randomUUID().toString(), "localhost", Long.MAX_VALUE);
    }

    public Session(String id, String ipAddress, long ttl)
    {
        super(id, ipAddress);
        this.ttl = ttl;
    }

    public long getTtl()
    {
        return ttl;
    }
}
