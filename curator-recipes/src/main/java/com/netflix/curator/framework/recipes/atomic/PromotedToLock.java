package com.netflix.curator.framework.recipes.atomic;

import com.google.common.base.Preconditions;
import com.netflix.curator.RetryPolicy;
import com.netflix.curator.retry.RetryNTimes;
import java.util.concurrent.TimeUnit;

/**
 * Abstraction of arguments for mutex promotion. Use {@link #builder()} to create.
 */
public class PromotedToLock
{
    private final String        path;
    private final long          maxLockTime;
    private final TimeUnit      maxLockTimeUnit;
    private final RetryPolicy   retryPolicy;

    /**
     * Allocate a new builder
     *
     * @return new builder
     */
    public static Builder   builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private PromotedToLock      instance = new PromotedToLock(null, -1, null, new RetryNTimes(0, 0));

        /**
         * Build the argument block
         *
         * @return new block
         */
        public PromotedToLock       build()
        {
            Preconditions.checkNotNull(instance.path);
            Preconditions.checkNotNull(instance.retryPolicy);

            return new PromotedToLock(instance.path, instance.maxLockTime, instance.maxLockTimeUnit, instance.retryPolicy);
        }

        /**
         * Set the path for the mutex lock (required)
         *
         * @param path path
         * @return this
         */
        public Builder          lockPath(String path)
        {
            instance = new PromotedToLock(path, instance.maxLockTime, instance.maxLockTimeUnit, instance.retryPolicy);
            return this;
        }

        /**
         * Set the retry policy to use when an operation does not succeeed (optional)
         *
         * @param retryPolicy new policy
         * @return this
         */
        public Builder          retryPolicy(RetryPolicy retryPolicy)
        {
            instance = new PromotedToLock(instance.path, instance.maxLockTime, instance.maxLockTimeUnit, retryPolicy);
            return this;
        }

        /**
         * Set the timeout to use when locking (optional)
         *
         * @param maxLockTime time
         * @param maxLockTimeUnit unit
         * @return this
         */
        public Builder          timeout(long maxLockTime, TimeUnit maxLockTimeUnit)
        {
            instance = new PromotedToLock(instance.path, maxLockTime, maxLockTimeUnit, instance.retryPolicy);
            return this;
        }

        private Builder()
        {
        }
    }

    String getPath()
    {
        return path;
    }

    long getMaxLockTime()
    {
        return maxLockTime;
    }

    TimeUnit getMaxLockTimeUnit()
    {
        return maxLockTimeUnit;
    }

    RetryPolicy getRetryPolicy()
    {
        return retryPolicy;
    }

    private PromotedToLock(String path, long maxLockTime, TimeUnit maxLockTimeUnit, RetryPolicy retryPolicy)
    {
        this.path = path;
        this.maxLockTime = maxLockTime;
        this.maxLockTimeUnit = maxLockTimeUnit;
        this.retryPolicy = retryPolicy;
    }
}
