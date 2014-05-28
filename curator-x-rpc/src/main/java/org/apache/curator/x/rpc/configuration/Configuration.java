package org.apache.curator.x.rpc.configuration;

import com.facebook.swift.service.ThriftServerConfig;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Configuration extends ThriftServerConfig
{
    private Duration projectionExpiration = new Duration(3, TimeUnit.MINUTES);
    private Duration pingTime = new Duration(5, TimeUnit.SECONDS);
    private List<String> connectionNames = ImmutableList.of();

    public Duration getProjectionExpiration()
    {
        return projectionExpiration;
    }

    @Config("curator.projection-expiration")
    @ConfigDescription("Curator projection instances will be expired after this amount of inactivity - default is 3 minutes")
    public void setProjectionExpiration(Duration projectionExpiration)
    {
        this.projectionExpiration = projectionExpiration;
    }

    public Duration getPingTime()
    {
        return pingTime;
    }

    @Config("curator.ping-time")
    @ConfigDescription("Calls to getNextEvent() will return PING after this duration - default is 5 seconds")
    public void setPingTime(Duration pingTime)
    {
        this.pingTime = pingTime;
    }

    public List<String> getConnectionNames()
    {
        return connectionNames;
    }

    @Config("curator.connection.names")
    public void setConnectionNames(String connectionNames)
    {
        this.connectionNames = Splitter.on(",").trimResults().splitToList(connectionNames);
    }
}
