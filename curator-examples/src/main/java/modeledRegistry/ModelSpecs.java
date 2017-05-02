package modeledRegistry;

import modeledRegistry.models.Cache;
import modeledRegistry.models.Database;
import modeledRegistry.models.InstanceId;
import modeledRegistry.models.Session;
import modeledRegistry.models.Zone;
import org.apache.curator.x.async.modeled.JacksonModelSerializer;
import org.apache.curator.x.async.modeled.ModelSpec;
import org.apache.curator.x.async.modeled.typed.TypedModelSpec;
import org.apache.curator.x.async.modeled.typed.TypedModelSpec2;

import static modeledRegistry.Paths.cachePath;
import static modeledRegistry.Paths.databasePath;
import static modeledRegistry.Paths.sessionPath;

public class ModelSpecs
{
    public static final TypedModelSpec2<Cache, Zone, InstanceId> cacheModelSpec = TypedModelSpec2.from(
        ModelSpec.builder(JacksonModelSerializer.build(Cache.class)).withNodeName(Cache::getId),
        cachePath
    );

    public static final TypedModelSpec2<Session, Zone, InstanceId> sessionModelSpec = TypedModelSpec2.from(
        ModelSpec.builder(JacksonModelSerializer.build(Session.class)).withNodeName(Session::getId),
        sessionPath
    );

    public static final TypedModelSpec<Database, Zone> databaseModelSpec = TypedModelSpec.from(
        ModelSpec.builder(JacksonModelSerializer.build(Database.class)).withNodeName(Database::getServerName),
        databasePath
    );

    private ModelSpecs()
    {
    }
}
