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

package org.apache.curator.framework.recipes.barriers;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import java.util.concurrent.TimeUnit;
import org.apache.curator.utils.PathUtils;

/**
 * <p>
 *     A barrier as described in the ZK recipes. Quoting the recipe:
 * </p>
 *
 * <blockquote>
 *     Distributed systems use barriers to block processing of a set of nodes
 *     until a condition is met at which time all the nodes are allowed to proceed
 * </blockquote>
 */
public class DistributedBarrier
{
    private final CuratorFramework client;
    private final String barrierPath;
    private final Watcher watcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            client.postSafeNotify(DistributedBarrier.this);
        }
    };

    /**
     * @param client client
     * @param barrierPath path to use as the barrier
     */
    public DistributedBarrier(CuratorFramework client, String barrierPath)
    {
        this.client = client;
        this.barrierPath = PathUtils.validatePath(barrierPath);
    }

    /**
     * Utility to set the barrier node
     *
     * @throws Exception errors
     */
    public synchronized void         setBarrier() throws Exception
    {
        try
        {
            client.create().creatingParentContainersIfNeeded().forPath(barrierPath);
        }
        catch ( KeeperException.NodeExistsException ignore )
        {
            // ignore
        }
    }

    /**
     * Utility to remove the barrier node
     *
     * @throws Exception errors
     */
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

    /**
     * Blocks until the barrier node comes into existence
     *
     * @throws Exception errors
     */
    public synchronized void      waitOnBarrier() throws Exception
    {
        waitOnBarrier(-1, null);
    }

    /**
     * Blocks until the barrier no longer exists or the timeout elapses
     *
     * @param maxWait max time to block
     * @param unit time unit
     * @return true if the wait was successful, false if the timeout elapsed first
     * @throws Exception errors
     */
    public synchronized boolean      waitOnBarrier(long maxWait, TimeUnit unit) throws Exception
    {
        long            startMs = System.currentTimeMillis();
        boolean         hasMaxWait = (unit != null);
        long            maxWaitMs = hasMaxWait ? TimeUnit.MILLISECONDS.convert(maxWait, unit) : Long.MAX_VALUE;

        boolean         result;
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
        return result;
    }
}
