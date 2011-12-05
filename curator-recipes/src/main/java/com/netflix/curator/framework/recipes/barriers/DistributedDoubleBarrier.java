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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;
import com.netflix.curator.utils.EnsurePath;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class DistributedDoubleBarrier
{
    private final CuratorFramework client;
    private final String barrierPath;
    private final int memberQty;
    private final String ourPath;
    private final String readyPath;
    private final EnsurePath ensurePath;
    private final AtomicBoolean hasBeenNotified = new AtomicBoolean(false);
    private final AtomicBoolean connectionLost = new AtomicBoolean(false);
    private final Watcher watcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            notifyFromWatcher();
        }
    };
    private final ConnectionStateListener   connectionStateListener = new ConnectionStateListener()
    {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
            if ( newState == ConnectionState.LOST )
            {
                connectionLost.set(true);
                notifyFromWatcher();
            }
        }
    };

    private static final String     READY_NODE = "ready";

    public DistributedDoubleBarrier(CuratorFramework client, String barrierPath, int memberQty)
    {
        Preconditions.checkArgument(memberQty > 0);

        this.client = client;
        this.barrierPath = barrierPath;
        this.memberQty = memberQty;
        ourPath = ZKPaths.makePath(barrierPath, UUID.randomUUID().toString());
        readyPath = ZKPaths.makePath(barrierPath, READY_NODE);
        ensurePath = client.newNamespaceAwareEnsurePath(barrierPath);
    }

    public void     enter() throws Exception
    {
        enter(-1, null);
    }

    public boolean     enter(long maxWait, TimeUnit unit) throws Exception
    {
        long            startMs = System.currentTimeMillis();
        boolean         hasMaxWait = (unit != null);
        long            maxWaitMs = hasMaxWait ? TimeUnit.MILLISECONDS.convert(maxWait, unit) : Long.MAX_VALUE;

        ensurePath.ensure(client.getZookeeperClient());

        boolean         readyPathExists = (client.checkExists().usingWatcher(watcher).forPath(readyPath) != null);
        client.create().withMode(CreateMode.EPHEMERAL).forPath(ourPath);

        boolean         result;
        client.getConnectionStateListenable().addListener(connectionStateListener);
        try
        {
            result = readyPathExists || internalEnter(startMs, hasMaxWait, maxWaitMs);
        }
        finally
        {
            client.getConnectionStateListenable().removeListener(connectionStateListener);
        }

        if ( connectionLost.get() )
        {
            throw new KeeperException.ConnectionLossException();
        }

        return result;
    }

    public synchronized void     leave() throws Exception
    {
        leave(-1, null);
    }

    public synchronized boolean     leave(long maxWait, TimeUnit unit) throws Exception
    {
        long            startMs = System.currentTimeMillis();
        boolean         hasMaxWait = (unit != null);
        long            maxWaitMs = hasMaxWait ? TimeUnit.MILLISECONDS.convert(maxWait, unit) : Long.MAX_VALUE;

        ensurePath.ensure(client.getZookeeperClient());

        boolean         result;
        client.getConnectionStateListenable().addListener(connectionStateListener);
        try
        {
            result = internalLeave(startMs, hasMaxWait, maxWaitMs);
        }
        finally
        {
            client.getConnectionStateListenable().removeListener(connectionStateListener);
        }

        return result;
    }

    private List<String> filterAndSortChildren(List<String> children)
    {
        Iterable<String> filtered = Iterables.filter
        (
            children,
            new Predicate<String>()
            {
                @Override
                public boolean apply(String name)
                {
                    return !name.equals(READY_NODE);
                }
            }
        );

        ArrayList<String> filteredList = Lists.newArrayList(filtered);
        Collections.sort(filteredList);
        return filteredList;
    }

    private boolean internalLeave(long startMs, boolean hasMaxWait, long maxWaitMs) throws Exception
    {
        String          ourPathName = ZKPaths.getNodeFromPath(ourPath);
        boolean         ourNodeShouldExist = true;
        boolean         result = true;
        for(;;)
        {
            if ( connectionLost.get() )
            {
                throw new KeeperException.ConnectionLossException();
            }

            List<String> children = client.getChildren().forPath(barrierPath);
            children = filterAndSortChildren(children);
            if ( (children == null) || (children.size() == 0) )
            {
                break;
            }

            int                 ourIndex = children.indexOf(ourPathName);
            if ( (ourIndex < 0) && ourNodeShouldExist )
            {
                throw new IllegalStateException(String.format("Our path (%s) is missing", ourPathName));
            }

            if ( children.size() == 1 )
            {
                if ( !children.get(0).equals(ourPathName) )
                {
                    throw new IllegalStateException(String.format("Last path (%s) is not ours (%s)", children.get(0), ourPathName));
                }
                checkDeleteOurPath(ourNodeShouldExist);
                break;
            }

            Stat            stat;
            boolean         IsLowestNode = (ourIndex == 0);
            if ( IsLowestNode )
            {
                String  highestNodePath = ZKPaths.makePath(barrierPath, children.get(children.size() - 1));
                stat = client.checkExists().usingWatcher(watcher).forPath(highestNodePath);
            }
            else
            {
                String  lowestNodePath = ZKPaths.makePath(barrierPath, children.get(0));
                stat = client.checkExists().usingWatcher(watcher).forPath(lowestNodePath);

                checkDeleteOurPath(ourNodeShouldExist);
                ourNodeShouldExist = false;
            }

            if ( stat != null )
            {
                if ( hasMaxWait )
                {
                    long        elapsed = System.currentTimeMillis() - startMs;
                    long        thisWaitMs = maxWaitMs - elapsed;
                    if ( thisWaitMs <= 0 )
                    {
                        result = false;
                    }

                    wait(thisWaitMs);
                }
                else
                {
                    wait();
                }
            }
        }

        try
        {
            client.delete().forPath(readyPath);
        }
        catch ( KeeperException.NoNodeException ignore )
        {
            // ignore
        }

        return result;
    }

    private void checkDeleteOurPath(boolean shouldExist) throws Exception
    {
        if ( shouldExist )
        {
            client.delete().forPath(ourPath);
        }
    }

    private synchronized boolean internalEnter(long startMs, boolean hasMaxWait, long maxWaitMs) throws Exception
    {
        boolean result = true;
        do
        {
            List<String>    children = client.getChildren().forPath(barrierPath);
            int             count = (children != null) ? children.size() : 0;
            if ( count == memberQty )
            {
                try
                {
                    client.create().forPath(readyPath);
                }
                catch ( KeeperException.NodeExistsException ignore )
                {
                    // ignore
                }
                break;
            }

            if ( count > memberQty )
            {
                throw new IllegalStateException(String.format("Count (%d) is greater than memberQty (%d)", count, memberQty));
            }

            if ( hasMaxWait && !hasBeenNotified.get() )
            {
                long        elapsed = System.currentTimeMillis() - startMs;
                long        thisWaitMs = maxWaitMs - elapsed;
                if ( thisWaitMs <= 0 )
                {
                    result = false;
                }

                wait(thisWaitMs);

                if ( !hasBeenNotified.get() )
                {
                    result = false;
                }
            }
            else
            {
                wait();
            }
        } while ( false );

        return result;
    }

    private synchronized void notifyFromWatcher()
    {
        hasBeenNotified.set(true);
        notifyAll();
    }
}
