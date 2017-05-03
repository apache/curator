package pubsub.messages;

import pubsub.models.Message;
import pubsub.models.Priority;
import java.util.Objects;

public class UserCreated extends Message
{
    private final String name;
    private final String position;

    public UserCreated()
    {
        this(Priority.low, "","");
    }

    public UserCreated(Priority priority, String name, String position)
    {
        super(priority);
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.position = Objects.requireNonNull(position, "position cannot be null");
    }

    public UserCreated(String id, Priority priority, String name, String position)
    {
        super(id, priority);
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.position = Objects.requireNonNull(position, "position cannot be null");
    }

    public String getName()
    {
        return name;
    }

    public String getPosition()
    {
        return position;
    }

    @Override
    public String toString()
    {
        return "UserCreated{" + "name='" + name + '\'' + ", position='" + position + '\'' + "} " + super.toString();
    }
}
