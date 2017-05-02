package modeledRegistry;

import modeledRegistry.models.InstanceId;
import modeledRegistry.models.Zone;
import org.apache.curator.x.async.modeled.typed.TypedZPath;
import org.apache.curator.x.async.modeled.typed.TypedZPath2;

public class Paths
{
    public static final TypedZPath2<Zone, InstanceId> cachePath = TypedZPath2.from("/root/registry/{id}/{id}/caches");
    public static final TypedZPath2<Zone, InstanceId> sessionPath = TypedZPath2.from("/root/registry/{id}/{id}/sessions");
    public static final TypedZPath<Zone> databasePath = TypedZPath.from("/root/registry/{id}/dbs");

    private Paths()
    {
    }
}
