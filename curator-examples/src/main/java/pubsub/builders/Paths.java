package pubsub.builders;

import org.apache.curator.x.async.modeled.typed.TypedZPath;
import org.apache.curator.x.async.modeled.typed.TypedZPath2;
import pubsub.models.Group;
import pubsub.models.InstanceType;
import pubsub.models.Priority;

public class Paths
{
    private static final String basePath = "/root/pubsub";

    public static final TypedZPath2<Group, Priority> messagesPath = TypedZPath2.from(basePath + "/messages/{id}/{id}");
    public static final TypedZPath<InstanceType> instancesPath = TypedZPath.from(basePath + "/instances/{id}");

    private Paths()
    {
    }
}
