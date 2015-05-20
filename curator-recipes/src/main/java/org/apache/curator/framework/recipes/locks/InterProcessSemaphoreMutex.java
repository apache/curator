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
package org.apache.curator.framework.recipes.locks;

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.WatcherRemoveCuratorFramework;
import java.util.concurrent.TimeUnit;

/**
 * A NON re-entrant mutex that works across JVMs. Uses Zookeeper to hold the lock. All processes in all JVMs that
 * use the same lock path will achieve an inter-process critical section.
 */
public class InterProcessSemaphoreMutex implements InterProcessLock
{
    private final InterProcessSemaphoreV2 semaphore;
    private final WatcherRemoveCuratorFramework watcherRemoveClient;
    private volatile Lease lease;

    /**
     * @param client the client
     * @param path path for the lock
     */
    public InterProcessSemaphoreMutex(CuratorFramework client, String path)
    {
        watcherRemoveClient = client.newWatcherRemoveCuratorFramework();
        this.semaphore = new InterProcessSemaphoreV2(watcherRemoveClient, path, 1);
    }

    @Override
    public void acquire() throws Exception
    {
        lease = semaphore.acquire();
    }

    @Override
    public boolean acquire(long time, TimeUnit unit) throws Exception
    {
        Lease acquiredLease = semaphore.acquire(time, unit);
        if ( acquiredLease == null )
        {
            return false;   // important - don't overwrite lease field if couldn't be acquired
        }
        lease = acquiredLease;
        return true;
    }

    @Override
    public void release() throws Exception
    {
        Preconditions.checkState(lease != null, "Not acquired");

        try
        {
            lease.close();
            watcherRemoveClient.removeWatchers();
        }
        finally
        {
            lease = null;
        }
    }

    @Override
    public boolean isAcquiredInThisProcess()
    {
        return (lease != null);
    }
}
