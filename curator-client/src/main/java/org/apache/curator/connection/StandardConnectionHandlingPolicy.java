/**
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
package org.apache.curator.connection;

import com.google.common.base.Preconditions;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.RetryLoop;
import org.apache.curator.utils.ThreadUtils;
import java.util.concurrent.Callable;

/**
 * Curator's standard connection handling since 3.0.0
 *
 * @since 3.0.0
 */
public class StandardConnectionHandlingPolicy implements ConnectionHandlingPolicy
{
    private final int expirationPercent;

    public StandardConnectionHandlingPolicy()
    {
        this(100);
    }

    public StandardConnectionHandlingPolicy(int expirationPercent)
    {
        Preconditions.checkArgument((expirationPercent > 0) && (expirationPercent <= 100), "expirationPercent must be > 0 and <= 100");
        this.expirationPercent = expirationPercent;
    }

    @Override
    public int getSimulatedSessionExpirationPercent()
    {
        return expirationPercent;
    }

    @Override
    public <T> T callWithRetry(CuratorZookeeperClient client, Callable<T> proc) throws Exception
    {
        client.internalBlockUntilConnectedOrTimedOut();

        T result = null;
        RetryLoop retryLoop = client.newRetryLoop();
        while ( retryLoop.shouldContinue() )
        {
            try
            {
                result = proc.call();
                retryLoop.markComplete();
            }
            catch ( Exception e )
            {
                ThreadUtils.checkInterrupted(e);
                retryLoop.takeException(e);
            }
        }

        return result;
    }

    @Override
    public CheckTimeoutsResult checkTimeouts(Callable<String> hasNewConnectionString, long connectionStartMs, int sessionTimeoutMs, int connectionTimeoutMs) throws Exception
    {
        if ( hasNewConnectionString.call() != null )
        {
            return CheckTimeoutsResult.NEW_CONNECTION_STRING;
        }
        return CheckTimeoutsResult.NOP;
    }
}
