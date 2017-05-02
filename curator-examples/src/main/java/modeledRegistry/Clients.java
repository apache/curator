package modeledRegistry;

import modeledRegistry.models.Cache;
import modeledRegistry.models.Database;
import modeledRegistry.models.InstanceId;
import modeledRegistry.models.Session;
import modeledRegistry.models.Zone;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.async.modeled.ModeledCuratorFramework;

import static modeledRegistry.ModelSpecs.cacheModelSpec;
import static modeledRegistry.ModelSpecs.databaseModelSpec;
import static modeledRegistry.ModelSpecs.sessionModelSpec;

public class Clients
{
    public static ModeledCuratorFramework<Cache> cacheClient(CuratorFramework client, Zone zone, InstanceId id)
    {
        return ModeledCuratorFramework.wrap(client, cacheModelSpec.resolved(zone, id));
    }

    public static ModeledCuratorFramework<Session> sessionClient(CuratorFramework client, Zone zone, InstanceId id)
    {
        return ModeledCuratorFramework.wrap(client, sessionModelSpec.resolved(zone, id));
    }

    public static ModeledCuratorFramework<Database> databaseClient(CuratorFramework client, Zone zone)
    {
        return ModeledCuratorFramework.wrap(client, databaseModelSpec.resolved(zone));
    }

    private Clients()
    {
    }
}
