package modeledRegistry;

import modeledRegistry.models.Database;
import org.apache.curator.x.async.modeled.ModeledCuratorFramework;
import java.util.List;
import java.util.function.Consumer;

public class RegistryExamples
{
    public static void addDatabase(ModeledCuratorFramework<Database> client, Database database)
    {
        ModeledCuratorFramework<Database> resolved = client.resolved(database);
        resolved.set(database);
    }

    public static void getDatabases(ModeledCuratorFramework<Database> client, Consumer<List<Database>> consumer)
    {
//        client.children().thenCombine();
    }
}
