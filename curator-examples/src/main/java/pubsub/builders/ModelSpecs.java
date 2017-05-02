package pubsub.builders;

import org.apache.curator.x.async.modeled.JacksonModelSerializer;
import org.apache.curator.x.async.modeled.ModelSpec;
import org.apache.curator.x.async.modeled.typed.TypedModelSpec;
import org.apache.curator.x.async.modeled.typed.TypedModelSpec2;
import pubsub.models.Group;
import pubsub.models.Instance;
import pubsub.models.InstanceType;
import pubsub.models.Message;
import pubsub.models.Priority;

public class ModelSpecs
{
    public static final TypedModelSpec2<Message, Group, Priority> messageModelSpec = TypedModelSpec2.from(
        ModelSpec.builder(JacksonModelSerializer.build(Message.class)),
        Paths.messagesPath
    );

    public static final TypedModelSpec<Instance, InstanceType> instanceModelSpec = TypedModelSpec.from(
        ModelSpec.builder(JacksonModelSerializer.build(Instance.class)),
        Paths.instancesPath
    );

    private ModelSpecs()
    {
    }
}
