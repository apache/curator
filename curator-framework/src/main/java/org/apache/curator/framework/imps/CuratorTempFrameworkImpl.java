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
package org.apache.curator.framework.imps;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closeables;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorTempFramework;
import org.apache.curator.framework.api.TempGetDataBuilder;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class CuratorTempFrameworkImpl implements CuratorTempFramework
{
    private final CuratorFrameworkFactory.Builder   factory;
    private final long                              inactiveThresholdMs;

    // guarded by sync
    private CuratorFrameworkImpl                    client;

    // guarded by sync
    private ScheduledExecutorService                cleanup;

    // guarded by sync
    private long                                    lastAccess;

    public CuratorTempFrameworkImpl(CuratorFrameworkFactory.Builder factory, long inactiveThresholdMs)
    {
        this.factory = factory;
        this.inactiveThresholdMs = inactiveThresholdMs;
    }

    @Override
    public void close()
    {
        closeClient();
    }

    @Override
    public CuratorTransaction inTransaction() throws Exception
    {
        openConnectionIfNeeded();
        return new CuratorTransactionImpl(client);
    }

    @Override
    public TempGetDataBuilder getData() throws Exception
    {
        openConnectionIfNeeded();
        return new TempGetDataBuilderImpl(client);
    }

    @VisibleForTesting
    CuratorFrameworkImpl getClient()
    {
        return client;
    }

    @VisibleForTesting
    ScheduledExecutorService getCleanup()
    {
        return cleanup;
    }

    @VisibleForTesting
    synchronized void updateLastAccess()
    {
        lastAccess = System.currentTimeMillis();
    }

    private synchronized void openConnectionIfNeeded() throws Exception
    {
        if ( client == null )
        {
            client = (CuratorFrameworkImpl)factory.build(); // cast is safe - we control both sides of this
            client.start();
        }

        if ( cleanup == null )
        {
            ThreadFactory threadFactory = factory.getThreadFactory();
            cleanup = (threadFactory != null) ? Executors.newScheduledThreadPool(1, threadFactory) : Executors.newScheduledThreadPool(1);

            Runnable        command = new Runnable()
            {
                @Override
                public void run()
                {
                    checkInactive();
                }
            };
            cleanup.scheduleAtFixedRate(command, inactiveThresholdMs, inactiveThresholdMs, TimeUnit.MILLISECONDS);
        }

        updateLastAccess();
    }

    private synchronized void checkInactive()
    {
        long        elapsed = System.currentTimeMillis() - lastAccess;
        if ( elapsed >= inactiveThresholdMs )
        {
            closeClient();
        }
    }

    private synchronized void closeClient()
    {
        if ( cleanup != null )
        {
            cleanup.shutdownNow();
            cleanup = null;
        }

        if ( client != null )
        {
            Closeables.closeQuietly(client);
            client = null;
        }
    }
}
