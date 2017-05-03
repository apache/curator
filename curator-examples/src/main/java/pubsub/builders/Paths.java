package pubsub.builders;

import org.apache.curator.x.async.modeled.typed.TypedZPath;
import org.apache.curator.x.async.modeled.typed.TypedZPath2;
import pubsub.models.Group;
import pubsub.models.InstanceType;
import pubsub.models.Priority;

public class Paths
{
    private static final String basePath = "/root/pubsub";

    public static final TypedZPath2<Group, Priority> locationAvailablePath = TypedZPath2.from(basePath + "/messages/locations/{id}/{id}");

    public static final TypedZPath2<Group, Priority> userCreatedPath = TypedZPath2.from(basePath + "/messages/users/{id}/{id}");

    public static final TypedZPath<InstanceType> instancesPath = TypedZPath.from(basePath + "/instances/{id}");

    private Paths()
    {
    }
}
