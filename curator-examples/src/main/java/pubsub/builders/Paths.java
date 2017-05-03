package pubsub.builders;

import org.apache.curator.x.async.modeled.typed.TypedZPath;
import org.apache.curator.x.async.modeled.typed.TypedZPath2;
import pubsub.models.Group;
import pubsub.models.InstanceType;
import pubsub.models.Priority;

public class Paths
{
    private static final String basePath = "/root/pubsub";

    /**
     * Represents a path for LocationAvailable models that is parameterized with a Group and a Priority
     */
    public static final TypedZPath2<Group, Priority> locationAvailablePath = TypedZPath2.from(basePath + "/messages/locations/{id}/{id}");

    /**
     * Represents a path for UserCreated models that is parameterized with a Group and a Priority
     */
    public static final TypedZPath2<Group, Priority> userCreatedPath = TypedZPath2.from(basePath + "/messages/users/{id}/{id}");

    /**
     * Represents a path for Instance models that is parameterized with a InstanceType
     */
    public static final TypedZPath<InstanceType> instancesPath = TypedZPath.from(basePath + "/instances/{id}");

    private Paths()
    {
    }
}
