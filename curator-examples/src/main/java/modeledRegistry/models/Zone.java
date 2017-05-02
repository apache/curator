package modeledRegistry.models;

import java.util.Objects;
import java.util.UUID;

public class Zone
{
    private final String id;
    private final String name;

    public Zone()
    {
        this(UUID.randomUUID().toString(), "");
    }

    public Zone(String name)
    {
        this(UUID.randomUUID().toString(), name);
    }

    public Zone(String id, String name)
    {
        this.id = Objects.requireNonNull(id, "id cannot be null");
        this.name = Objects.requireNonNull(name, "name cannot be null");
    }

    public String getId()
    {
        return id;
    }

    public String getName()
    {
        return name;
    }
}
