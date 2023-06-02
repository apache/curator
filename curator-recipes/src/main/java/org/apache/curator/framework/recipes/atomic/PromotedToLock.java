/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.curator.framework.recipes.atomic;

import com.google.common.base.Preconditions;
import java.util.concurrent.TimeUnit;
import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.PathUtils;

/**
 * Abstraction of arguments for mutex promotion. Use {@link #builder()} to create.
 */
public class PromotedToLock {
    private final String path;
    private final long maxLockTime;
    private final TimeUnit maxLockTimeUnit;
    private final RetryPolicy retryPolicy;

    /**
     * Allocate a new builder
     *
     * @return new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private PromotedToLock instance = new PromotedToLock(null, -1, null, new RetryNTimes(0, 0));

        /**
         * Build the argument block
         *
         * @return new block
         */
        public PromotedToLock build() {
            Preconditions.checkNotNull(instance.path, "path cannot be null");
            Preconditions.checkNotNull(instance.retryPolicy, "retryPolicy cannot be null");

            return new PromotedToLock(
                    instance.path, instance.maxLockTime, instance.maxLockTimeUnit, instance.retryPolicy);
        }

        /**
         * Set the path for the mutex lock (required)
         *
         * @param path path
         * @return this
         */
        public Builder lockPath(String path) {
            instance = new PromotedToLock(
                    PathUtils.validatePath(path), instance.maxLockTime, instance.maxLockTimeUnit, instance.retryPolicy);
            return this;
        }

        /**
         * Set the retry policy to use when an operation does not succeed
         *
         * @param retryPolicy new policy
         * @return this
         */
        public Builder retryPolicy(RetryPolicy retryPolicy) {
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
        public Builder timeout(long maxLockTime, TimeUnit maxLockTimeUnit) {
            instance = new PromotedToLock(instance.path, maxLockTime, maxLockTimeUnit, instance.retryPolicy);
            return this;
        }

        private Builder() {}
    }

    String getPath() {
        return path;
    }

    long getMaxLockTime() {
        return maxLockTime;
    }

    TimeUnit getMaxLockTimeUnit() {
        return maxLockTimeUnit;
    }

    RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    private PromotedToLock(String path, long maxLockTime, TimeUnit maxLockTimeUnit, RetryPolicy retryPolicy) {
        this.path = path;
        this.maxLockTime = maxLockTime;
        this.maxLockTimeUnit = maxLockTimeUnit;
        this.retryPolicy = retryPolicy;
    }
}
