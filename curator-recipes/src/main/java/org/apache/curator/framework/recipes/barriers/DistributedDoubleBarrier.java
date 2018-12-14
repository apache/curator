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
package org.apache.curator.framework.recipes.barriers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
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
import org.apache.curator.utils.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 *     A double barrier as described in the ZK recipes. Quoting the recipe:
 * </p>
 *
 * <blockquote>
 *     Double barriers enable
 *     clients to synchronize the beginning and the end of a computation. When enough processes
 *     have joined the barrier, processes start their computation and leave the barrier
 *     once they have finished.
 * </blockquote>
 */
public class DistributedDoubleBarrier
{
    private static final Logger logger = LoggerFactory.getLogger(DistributedDoubleBarrier.class);

    private final CuratorFramework client;
    private final String barrierPath;
    private final int memberQty;
    private final String ourPath;
    private final String readyPath;
    private final AtomicBoolean hasBeenNotifiedEnter = new AtomicBoolean(false);
    private final AtomicBoolean hasBeenNotifiedLeave = new AtomicBoolean(false);
    private final AtomicBoolean connectionLost = new AtomicBoolean(false);
    private final Watcher watcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            connectionLost.set(event.getState() != Event.KeeperState.SyncConnected);
            notifyFromWatcher();
        }
    };

    private static final String READY_NODE = "ready";

    /**
     * Creates the barrier abstraction. <code>memberQty</code> is the number of members in the
     * barrier. When {@link #enter()} is called, it blocks until all members have entered. When
     * {@link #leave()} is called, it blocks until all members have left.
     *
     * @param client the client
     * @param barrierPath path to use
     * @param memberQty the number of members in the barrier. NOTE: more than <code>memberQty</code>
     *                  can enter the barrier. <code>memberQty</code> is a threshold, not a limit
     */
    public DistributedDoubleBarrier(CuratorFramework client, String barrierPath, int memberQty)
    {
        Preconditions.checkState(memberQty > 0, "memberQty cannot be 0");

        this.client = client;
        this.barrierPath = PathUtils.validatePath(barrierPath);
        this.memberQty = memberQty;
        ourPath = ZKPaths.makePath(barrierPath, UUID.randomUUID().toString());
        readyPath = ZKPaths.makePath(barrierPath, READY_NODE);
    }

    /**
     * Enter the barrier and block until all members have entered
     *
     * @throws Exception interruptions, errors, etc.
     */
    public void     enter() throws Exception
    {
        enter(-1, null);
    }

    /**
     * Enter the barrier and block until all members have entered or the timeout has
     * elapsed
     *
     * @param maxWait max time to block
     * @param unit time unit
     * @return true if the entry was successful, false if the timeout elapsed first
     * @throws Exception interruptions, errors, etc.
     */
    public boolean     enter(long maxWait, TimeUnit unit) throws Exception
    {
        long            startMs = System.currentTimeMillis();
        boolean         hasMaxWait = (unit != null);
        long            maxWaitMs = hasMaxWait ? TimeUnit.MILLISECONDS.convert(maxWait, unit) : Long.MAX_VALUE;

        boolean         readyPathExists = (client.checkExists().usingWatcher(watcher).forPath(readyPath) != null);
        client.create().creatingParentContainersIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(ourPath);

        boolean         result = (readyPathExists || internalEnter(startMs, hasMaxWait, maxWaitMs));
        if ( connectionLost.get() )
        {
            throw new KeeperException.ConnectionLossException();
        }

        return result;
    }

    /**
     * Leave the barrier and block until all members have left
     *
     * @throws Exception interruptions, errors, etc.
     */
    public synchronized void     leave() throws Exception
    {
        leave(-1, null);
    }

    /**
     * Leave the barrier and block until all members have left or the timeout has
     * elapsed
     *
     * @param maxWait max time to block
     * @param unit time unit
     * @return true if leaving was successful, false if the timeout elapsed first
     * @throws Exception interruptions, errors, etc.
     */
    public synchronized boolean     leave(long maxWait, TimeUnit unit) throws Exception
    {
        long            startMs = System.currentTimeMillis();
        boolean         hasMaxWait = (unit != null);
        long            maxWaitMs = hasMaxWait ? TimeUnit.MILLISECONDS.convert(maxWait, unit) : Long.MAX_VALUE;

        return internalLeave(startMs, hasMaxWait, maxWaitMs);
    }

    @VisibleForTesting
    protected List<String> getChildrenForEntering() throws Exception
    {
        return client.getChildren().forPath(barrierPath);
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
        logger.trace(">>> internalLeave {}", ourPath);

        String          ourPathName = ZKPaths.getNodeFromPath(ourPath);
        boolean         ourNodeShouldExist = true;
        boolean         result = true;

        hasBeenNotifiedLeave.set(false);

        for ( ;; )
        {
            if ( connectionLost.get() )
            {
                throw new KeeperException.ConnectionLossException();
            }

            List<String> children;
            try
            {
                children = client.getChildren().forPath(barrierPath);
            }
            catch ( KeeperException.NoNodeException dummy )
            {
                children = Lists.newArrayList();
            }
            children = filterAndSortChildren(children);
            if ( (children == null) || (children.size() == 0) )
            {
                break;
            }

            int                 ourIndex = children.indexOf(ourPathName);
            if ( (ourIndex < 0) && ourNodeShouldExist )
            {
                if ( connectionLost.get() )
                {
                    break;  // connection was lost but we've reconnected. However, our ephemeral node is gone
                }
                else
                {
                    throw new IllegalStateException(String.format("Our path (%s) is missing", ourPathName));
                }
            }

            logger.trace("children:{}", children);
            if ( children.size() == 1 )
            {
                if ( ourNodeShouldExist && !children.get(0).equals(ourPathName) )
                {
                    throw new IllegalStateException(String.format("Last path (%s) is not ours (%s)", children.get(0), ourPathName));
                }
                checkDeleteOurPath(ourNodeShouldExist);
                break;
            }

            String watchPath; // Watch somebody else that still exists
            if ( ourIndex == 0 )
            {
                watchPath = ZKPaths.makePath(barrierPath, children.get(children.size() - 1));
            }
            else
            {
                watchPath = ZKPaths.makePath(barrierPath, children.get(0));
            }

            Stat stat = client.checkExists().usingWatcher(watcher).forPath(watchPath);

            checkDeleteOurPath(ourNodeShouldExist);
            ourNodeShouldExist = false;

            if ( stat != null )
            {
                if ( hasMaxWait )
                {
                    result = timedWait(startMs, maxWaitMs, hasBeenNotifiedLeave);
                    if ( !result )
                    {
                        break;
                    }
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
        hasBeenNotifiedEnter.set(false);
        boolean result = true;

        List<String> children = getChildrenForEntering();
        int             count = (children != null) ? children.size() : 0;
        if ( count >= memberQty )
        {
            try
            {
                client.create().forPath(readyPath);
            }
            catch ( KeeperException.NodeExistsException ignore )
            {
                // ignore
            }
        }
        else
        {
            if ( hasMaxWait )
            {
                result = timedWait(startMs, maxWaitMs, hasBeenNotifiedEnter);
            }
            else
            {
                wait();
            }
        }

        return result;
    }

    private boolean timedWait(long startMs, long maxWaitMs, AtomicBoolean notified) throws InterruptedException
    {
        long elapsed = System.currentTimeMillis() - startMs;
        while ( !notified.get() && elapsed < maxWaitMs )
        {
            wait(maxWaitMs - elapsed);
            elapsed = System.currentTimeMillis() - startMs;
            logger.trace("max:{} elapsed:{}", maxWaitMs, elapsed);
        }

        return notified.get();
    }

    private synchronized void notifyFromWatcher()
    {
        hasBeenNotifiedEnter.set(true);
        hasBeenNotifiedLeave.set(true);
        notifyAll();
    }
}
