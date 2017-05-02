package modeledRegistry.models;

import java.util.Objects;
import java.util.UUID;

public class Database extends Server
{
    private final String connectionString;
    private final String serverName;

    public Database()
    {
        this(UUID.randomUUID().toString(), "localhost", "", "");
    }

    public Database(String id, String ipAddress, String serverName, String connectionString)
    {
        super(id, ipAddress);
        this.serverName = Objects.requireNonNull(serverName, "serverName cannot be null");
        this.connectionString = Objects.requireNonNull(connectionString, "connectionString cannot be null");
    }

    public String getConnectionString()
    {
        return connectionString;
    }

    public String getServerName()
    {
        return serverName;
    }
}
