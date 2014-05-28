package org.apache.curator.x.rpc.configuration;

import io.airlift.units.Duration;
import javax.validation.constraints.NotNull;
import java.util.concurrent.TimeUnit;

public class ConnectionConfiguration
{
    @NotNull private String name;
    private String connectionString = "localhost:2181";
    private Duration sessionLength = new Duration(1, TimeUnit.MINUTES);
    private Duration connectionTimeout = new Duration(15, TimeUnit.SECONDS);
    private AuthorizationConfiguration authorization = null;
    private String namespace = null;
    private RetryPolicyConfiguration retry = new ExponentialBackoffRetryConfiguration();

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getConnectionString()
    {
        return connectionString;
    }

    public void setConnectionString(String connectionString)
    {
        this.connectionString = connectionString;
    }

    public Duration getSessionLength()
    {
        return sessionLength;
    }

    public void setSessionLength(Duration sessionLength)
    {
        this.sessionLength = sessionLength;
    }

    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    public void setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
    }

    public AuthorizationConfiguration getAuthorization()
    {
        return authorization;
    }

    public void setAuthorization(AuthorizationConfiguration authorization)
    {
        this.authorization = authorization;
    }

    public String getNamespace()
    {
        return namespace;
    }

    public void setNamespace(String namespace)
    {
        this.namespace = namespace;
    }

    public RetryPolicyConfiguration getRetry()
    {
        return retry;
    }

    public void setRetry(RetryPolicyConfiguration retry)
    {
        this.retry = retry;
    }
}
