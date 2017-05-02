package modeledRegistry.models;

import java.util.Objects;
import java.util.UUID;

public class InstanceId
{
    private final String id;

    public InstanceId()
    {
        this(UUID.randomUUID().toString());
    }

    public InstanceId(String id)
    {
        this.id = Objects.requireNonNull(id, "id cannot be null");
    }

    public String getId()
    {
        return id;
    }
}
