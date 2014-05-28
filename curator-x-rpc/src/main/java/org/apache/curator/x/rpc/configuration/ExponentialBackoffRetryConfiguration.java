package org.apache.curator.x.rpc.configuration;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;

public class ExponentialBackoffRetryConfiguration
{
    private Duration baseSleepTime;
    private int maxRetries;
    private Duration maxSleep;

    public Duration getBaseSleepTime()
    {
        return baseSleepTime;
    }

    @Config("curator.retry.exponential.base-sleep-time")
    public void setBaseSleepTime(Duration baseSleepTime)
    {
        this.baseSleepTime = baseSleepTime;
    }

    public int getMaxRetries()
    {
        return maxRetries;
    }

    @Config("curator.retry.exponential.max-retries")
    public void setMaxRetries(int maxRetries)
    {
        this.maxRetries = maxRetries;
    }

    public Duration getMaxSleep()
    {
        return maxSleep;
    }

    @Config("curator.retry.exponential.max-sleep")
    public void setMaxSleep(Duration maxSleep)
    {
        this.maxSleep = maxSleep;
    }
}
