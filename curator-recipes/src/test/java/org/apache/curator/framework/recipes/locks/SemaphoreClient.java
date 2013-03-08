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

package org.apache.curator.framework.recipes.locks;

import com.google.common.io.Closeables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.Timing;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

class SemaphoreClient implements Callable<Void>, ConnectionStateListener, Closeable
{
    private final CuratorFramework client;
    private final String semaphorePath;
    private final Callable<Void> operation;

    private volatile boolean shouldRun;
    private volatile boolean hasAcquired;

    private static final int CLIENT_EXCEPTION_HANDLER_SLEEP_TIME_SECS = 10;
    private static final int MAX_SEMAPHORE_LEASES = 1;

    private static final AtomicReference<SemaphoreClient>       activeClient = new AtomicReference<SemaphoreClient>(null);

    SemaphoreClient(String connectionString, String semaphorePath, Callable<Void> operation) throws IOException
    {
        Timing timing = new Timing();
        this.client = CuratorFrameworkFactory.newClient(connectionString, timing.session(), timing.connection(), new ExponentialBackoffRetry(100, 3));
        client.start();

        this.semaphorePath = semaphorePath;
        this.operation = operation;
    }

    @Override
    public void close() throws IOException
    {
        shouldRun = false;
    }

    boolean hasAcquired()
    {
        return hasAcquired;
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState)
    {
        hasAcquired = false;
    }

    static SemaphoreClient getActiveClient()
    {
        return activeClient.get();
    }

    @Override
    public Void call() throws Exception
    {
        shouldRun = true;
        client.getConnectionStateListenable().addListener(this);
        try
        {
            while ( shouldRun )
            {
                try
                {
                    acquireAndRun();
                }
                catch ( InterruptedException e )
                {
                    Thread.currentThread().interrupt();
                    // propagate up, don't sleep
                    throw e;

                }
                catch ( Exception e )
                {
                    Thread.sleep(CLIENT_EXCEPTION_HANDLER_SLEEP_TIME_SECS * 1000L);
                }
            }

        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
        }
        finally
        {
            Closeables.closeQuietly(client);
        }
        return null;
    }

    private void acquireAndRun() throws Exception
    {
        InterProcessSemaphoreV2 semaphore = new InterProcessSemaphoreV2(client, semaphorePath, MAX_SEMAPHORE_LEASES);
        Lease lease = semaphore.acquire();
        try
        {
            hasAcquired = true;
            if ( activeClient.compareAndSet(null, this) )
            {
                throw new Exception("Multiple acquirers");
            }

            try
            {
                while ( hasAcquired && shouldRun )
                {
                    operation.call();
                }
            }
            finally
            {
                if ( activeClient.compareAndSet(this, null) )
                {
                    //noinspection ThrowFromFinallyBlock
                    throw new Exception("Bad release");
                }
            }
        }
        finally
        {
            semaphore.returnLease(lease);
        }
    }
}
