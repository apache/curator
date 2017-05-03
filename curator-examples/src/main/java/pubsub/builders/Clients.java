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
    /**
     * A client template for LocationAvailable instances
     */
    public static final TypedModeledFramework2<LocationAvailable, Group, Priority> locationAvailableClient = TypedModeledFramework2.from(
        ModeledFramework.builder(),             // our client will use only defaults
        ModelSpecs.locationAvailableModelSpec   // the LocationAvailable model spec
    );

    /**
     * A client template for UserCreated instances
     */
    public static final TypedModeledFramework2<UserCreated, Group, Priority> userCreatedClient = TypedModeledFramework2.from(
        ModeledFramework.builder(),             // our client will use only defaults
        ModelSpecs.userCreatedModelSpec         // the UserCreated model spec
    );

    /**
     * A client template for Instance instances
     */
    public static final TypedModeledFramework<Instance, InstanceType> instanceClient = TypedModeledFramework.from(
        ModeledFramework.builder(),             // our client will use only defaults
        ModelSpecs.instanceModelSpec            // the Instance model spec
    );

    private Clients()
    {
    }
}
