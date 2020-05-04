package org.apache.curator;

import org.apache.zookeeper.KeeperException;

/**
 * {@link RetryPolicy} implementation that failed on session expired.
 */
public class SessionFailedRetryPolicy implements RetryPolicy
{

    private final RetryPolicy delegatePolicy;

    public SessionFailedRetryPolicy(RetryPolicy delegatePolicy)
    {
        this.delegatePolicy = delegatePolicy;
    }

    @Override
    public boolean allowRetry(int retryCount, long elapsedTimeMs, RetrySleeper sleeper)
    {
        return delegatePolicy.allowRetry(retryCount, elapsedTimeMs, sleeper);
    }

    @Override
    public boolean allowRetry(Throwable exception)
    {
        if ( exception instanceof KeeperException.SessionExpiredException )
        {
            return false;
        }
        else
        {
            return delegatePolicy.allowRetry(exception);
        }
    }
}
