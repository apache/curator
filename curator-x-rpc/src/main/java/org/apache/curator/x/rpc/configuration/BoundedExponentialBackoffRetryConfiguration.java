package org.apache.curator.x.rpc.configuration;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.airlift.units.Duration;
import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import java.util.concurrent.TimeUnit;

@JsonTypeName("bounded-exponential-backoff")
public class BoundedExponentialBackoffRetryConfiguration extends RetryPolicyConfiguration
{
    private Duration baseSleepTime = new Duration(100, TimeUnit.MILLISECONDS);
    private Duration maxSleepTime = new Duration(30, TimeUnit.SECONDS);
    private int maxRetries = 3;

    @Override
    public RetryPolicy build()
    {
        return new BoundedExponentialBackoffRetry((int)baseSleepTime.toMillis(), (int)maxSleepTime.toMillis(), maxRetries);
    }

    public Duration getBaseSleepTime()
    {
        return baseSleepTime;
    }

    public void setBaseSleepTime(Duration baseSleepTime)
    {
        this.baseSleepTime = baseSleepTime;
    }

    public int getMaxRetries()
    {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries)
    {
        this.maxRetries = maxRetries;
    }

    public Duration getMaxSleepTime()
    {
        return maxSleepTime;
    }

    public void setMaxSleepTime(Duration maxSleepTime)
    {
        this.maxSleepTime = maxSleepTime;
    }
}
