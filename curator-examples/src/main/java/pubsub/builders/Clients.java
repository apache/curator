package pubsub.builders;

import org.apache.curator.x.async.modeled.ModeledFramework;
import org.apache.curator.x.async.modeled.typed.TypedModeledFramework;
import org.apache.curator.x.async.modeled.typed.TypedModeledFramework2;
import pubsub.messages.LocationAvailable;
import pubsub.messages.UserCreated;
import pubsub.models.Group;
import pubsub.models.Instance;
import pubsub.models.InstanceType;
import pubsub.models.Priority;

public class Clients
{
    public static final TypedModeledFramework2<LocationAvailable, Group, Priority> locationAvailableClient = TypedModeledFramework2.from(
        ModeledFramework.builder(),
        ModelSpecs.locationAvailableModelSpec
    );

    public static final TypedModeledFramework2<UserCreated, Group, Priority> userCreatedClient = TypedModeledFramework2.from(
        ModeledFramework.builder(),
        ModelSpecs.userCreatedModelSpec
    );

    public static final TypedModeledFramework<Instance, InstanceType> instanceClient = TypedModeledFramework.from(
        ModeledFramework.builder(),
        ModelSpecs.instanceModelSpec
    );

    private Clients()
    {
    }
}
