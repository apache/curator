package org.apache.curator.x.rpc.configuration;

import com.facebook.swift.service.ThriftServerConfig;
import io.airlift.units.Duration;
import io.dropwizard.logging.LoggingFactory;
import java.util.concurrent.TimeUnit;

public class ConfigurationX
{
    private ThriftServerConfig thrift = new ThriftServerConfig();
    private LoggingFactory logging = new LoggingFactory();
    private Duration projectionExpiration = new Duration(3, TimeUnit.MINUTES);
    private Duration pingTime = new Duration(5, TimeUnit.SECONDS);

    public LoggingFactory getLogging()
    {
        return logging;
    }

    public void setLogging(LoggingFactory logging)
    {
        this.logging = logging;
    }

    public ThriftServerConfig getThrift()
    {
        return thrift;
    }

    public void setThrift(ThriftServerConfig thrift)
    {
        this.thrift = thrift;
    }

    public Duration getProjectionExpiration()
    {
        return projectionExpiration;
    }

    public void setProjectionExpiration(Duration projectionExpiration)
    {
        this.projectionExpiration = projectionExpiration;
    }

    public Duration getPingTime()
    {
        return pingTime;
    }

    public void setPingTime(Duration pingTime)
    {
        this.pingTime = pingTime;
    }
}
