package pubsub.builders;

import org.apache.curator.x.async.modeled.ModeledFramework;
import org.apache.curator.x.async.modeled.typed.TypedModeledFramework;
import org.apache.curator.x.async.modeled.typed.TypedModeledFramework2;
import pubsub.models.Group;
import pubsub.models.Instance;
import pubsub.models.InstanceType;
import pubsub.models.Message;
import pubsub.models.Priority;

public class Clients
{
    public static final TypedModeledFramework2<Message, Group, Priority> messageClient = TypedModeledFramework2.from(
        ModeledFramework.builder(),
        ModelSpecs.messageModelSpec
    );

    public static final TypedModeledFramework<Instance, InstanceType> instanceClient = TypedModeledFramework.from(
        ModeledFramework.builder(),
        ModelSpecs.instanceModelSpec
    );

    private Clients()
    {
    }
}
