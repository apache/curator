package org.apache.curator.x.rpc.configuration;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.airlift.units.Duration;
import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.RetryNTimes;
import java.util.concurrent.TimeUnit;

@JsonTypeName("ntimes")
public class RetryNTimesConfiguration extends RetryPolicyConfiguration
{
    private Duration sleepBetweenRetries = new Duration(100, TimeUnit.MILLISECONDS);
    private int n = 3;

    @Override
    public RetryPolicy build()
    {
        return new RetryNTimes(n, (int)sleepBetweenRetries.toMillis());
    }

    public Duration getSleepBetweenRetries()
    {
        return sleepBetweenRetries;
    }

    public void setSleepBetweenRetries(Duration sleepBetweenRetries)
    {
        this.sleepBetweenRetries = sleepBetweenRetries;
    }

    public int getN()
    {
        return n;
    }

    public void setN(int n)
    {
        this.n = n;
    }
}
