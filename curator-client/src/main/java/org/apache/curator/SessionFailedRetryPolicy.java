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

package org.apache.curator;

import org.apache.zookeeper.KeeperException;

/**
 * {@link RetryPolicy} implementation that failed on session expired.
 */
public class SessionFailedRetryPolicy implements RetryPolicy {

    private final RetryPolicy delegatePolicy;

    public SessionFailedRetryPolicy(RetryPolicy delegatePolicy) {
        this.delegatePolicy = delegatePolicy;
    }

    @Override
    public boolean allowRetry(int retryCount, long elapsedTimeMs, RetrySleeper sleeper) {
        return delegatePolicy.allowRetry(retryCount, elapsedTimeMs, sleeper);
    }

    @Override
    public boolean allowRetry(Throwable exception) {
        if (exception instanceof KeeperException.SessionExpiredException) {
            return false;
        } else {
            return delegatePolicy.allowRetry(exception);
        }
    }
}
