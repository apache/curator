package modeledRegistry.models;

import java.util.UUID;

public class Cache extends Server
{
    private final int maxObjects;

    public Cache()
    {
        this(UUID.randomUUID().toString(), "localhost", Integer.MAX_VALUE);
    }

    public Cache(String id, String ipAddress, int maxObjects)
    {
        super(id, ipAddress);
        this.maxObjects = maxObjects;
    }

    public int getMaxObjects()
    {
        return maxObjects;
    }
}
