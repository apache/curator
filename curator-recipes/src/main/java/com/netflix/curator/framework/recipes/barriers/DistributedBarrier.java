/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.curator.framework.recipes.barriers;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import java.util.concurrent.TimeUnit;

public class DistributedBarrier
{
    private final CuratorFramework client;
    private final String barrierPath;
    private final Watcher watcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            notifyFromWatcher();
        }
    };
    private final ConnectionStateListener connectionStateListener = new ConnectionStateListener()
    {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
            notifyFromWatcher();
        }
    };

    public DistributedBarrier(CuratorFramework client, String barrierPath)
    {
        this.client = client;
        this.barrierPath = barrierPath;
    }

    public synchronized void         setBarrier() throws Exception
    {
        try
        {
            client.create().creatingParentsIfNeeded().forPath(barrierPath, new byte[0]);
        }
        catch ( KeeperException.NodeExistsException ignore )
        {
            // ignore
        }
    }

    public synchronized void      removeBarrier() throws Exception
    {
        try
        {
            client.delete().forPath(barrierPath);
        }
        catch ( KeeperException.NoNodeException ignore )
        {
            // ignore
        }
    }

    public synchronized void      waitOnBarrier() throws Exception
    {
        waitOnBarrier(-1, null);
    }

    public synchronized boolean      waitOnBarrier(long maxWait, TimeUnit unit) throws Exception
    {
        long            startMs = System.currentTimeMillis();
        boolean         hasMaxWait = (unit != null);
        long            maxWaitMs = hasMaxWait ? TimeUnit.MILLISECONDS.convert(maxWait, unit) : Long.MAX_VALUE;

        boolean         result;
        client.getConnectionStateListenable().addListener(connectionStateListener);
        try
        {
            for(;;)
            {
                result = (client.checkExists().usingWatcher(watcher).forPath(barrierPath) == null);
                if ( result )
                {
                    break;
                }

                if ( hasMaxWait )
                {
                    long        elapsed = System.currentTimeMillis() - startMs;
                    long        thisWaitMs = maxWaitMs - elapsed;
                    if ( thisWaitMs <= 0 )
                    {
                        break;
                    }
                    wait(thisWaitMs);
                }
                else
                {
                    wait();
                }
            }
        }
        finally
        {
            client.getConnectionStateListenable().removeListener(connectionStateListener);
        }
        return result;
    }

    private synchronized void notifyFromWatcher()
    {
        notifyAll();
    }
}
