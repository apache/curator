package org.apache.curator.x.rpc.configuration;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import java.util.concurrent.TimeUnit;

public class ConnectionConfiguration
{
    private String connectionString = "localhost:2181";
    private Duration sessionLength = new Duration(1, TimeUnit.MINUTES);
    private Duration connectionTimeout = new Duration(15, TimeUnit.SECONDS);
    private RetryType retryType;

    public String getConnectionString()
    {
        return connectionString;
    }

    @Config("curator.connection.$CONNECTION-NAME$.connection-string")
    @ConfigDescription("Default ZooKeeper connection string. E.g. \"foo.com:2181,bar.com:2181\"")
    public void setConnectionString(String connectionString)
    {
        this.connectionString = connectionString;
    }

    public Duration getSessionLength()
    {
        return sessionLength;
    }

    @Config("curator.connection.$CONNECTION-NAME$.session-length")
    @ConfigDescription("Session length. Default is 1 minute")
    public void setSessionLength(Duration sessionLength)
    {
        this.sessionLength = sessionLength;
    }

    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("curator.connection.$CONNECTION-NAME$.connection-timeout")
    @ConfigDescription("Connection timeout. Default is 15 seconds")
    public void setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
    }

    public RetryType getRetryType()
    {
        return retryType;
    }

    @Config("curator.connection.$CONNECTION-NAME$.retry.type")
    public void setRetryType(RetryType retryType)
    {
        this.retryType = retryType;
    }
}
