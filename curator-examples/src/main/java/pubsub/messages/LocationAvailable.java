package pubsub.messages;

import pubsub.models.Message;
import pubsub.models.Priority;
import java.time.Duration;
import java.util.Objects;

public class LocationAvailable extends Message
{
    private final String name;
    private final Duration availableUntil;

    public LocationAvailable(Priority priority, String name, Duration availableUntil)
    {
        super(priority);
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.availableUntil = Objects.requireNonNull(availableUntil, "availableUntil cannot be null");
    }

    public LocationAvailable(String id, Priority priority, String name, Duration availableUntil)
    {
        super(id, priority);
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.availableUntil = Objects.requireNonNull(availableUntil, "availableUntil cannot be null");
    }
}
