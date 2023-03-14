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

package org.apache.curator.framework.recipes.queue;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.EnsureContainers;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *     Drop in replacement for: org.apache.zookeeper.recipes.queue.DistributedQueue that is part of
 *     the ZooKeeper distribution
 * </p>
 *
 * <p>
 *     This class is data compatible with the ZK version. i.e. it uses the same naming scheme so
 *     it can read from an existing queue
 * </p>
 */
public class SimpleDistributedQueue
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorFramework client;
    private final String path;
    private final EnsureContainers ensureContainers;

    private final String PREFIX = "qn-";

    /**
     * @param client the client
     * @param path path to store queue nodes
     */
    public SimpleDistributedQueue(CuratorFramework client, String path)
    {
        this.client = client;
        this.path = PathUtils.validatePath(path);
        ensureContainers = new EnsureContainers(client, path);
    }

    /**
     * Return the head of the queue without modifying the queue.
     *
     * @return the data at the head of the queue.
     * @throws Exception errors
     * @throws NoSuchElementException if the queue is empty
     */
    public byte[] element() throws Exception
    {
        byte[] bytes = internalElement(false, null);
        if ( bytes == null )
        {
            throw new NoSuchElementException();
        }
        return bytes;
    }

    /**
     * Attempts to remove the head of the queue and return it.
     *
     * @return The former head of the queue
     * @throws Exception errors
     * @throws NoSuchElementException if the queue is empty
     */
    public byte[] remove() throws Exception
    {
        byte[] bytes = internalElement(true, null);
        if ( bytes == null )
        {
            throw new NoSuchElementException();
        }
        return bytes;
    }

    /**
     * Removes the head of the queue and returns it, blocks until it succeeds.
     *
     * @return The former head of the queue
     * @throws Exception errors
     */
    public byte[] take() throws Exception
    {
        return internalPoll(0, null);
    }

    /**
     * Inserts data into queue.
     *
     * @param data the data
     * @return true if data was successfully added
     * @throws Exception errors
     */
    public boolean offer(byte[] data) throws Exception
    {
        String thisPath = ZKPaths.makePath(path, PREFIX);
        client.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(thisPath, data);
        return true;
    }

    /**
     * Returns the data at the first element of the queue, or null if the queue is empty.
     *
     * @return data at the first element of the queue, or null.
     * @throws Exception errors
     */
    public byte[] peek() throws Exception
    {
        try
        {
            return element();
        }
        catch ( NoSuchElementException e )
        {
            return null;
        }
    }

    /**
     * Retrieves and removes the head of this queue, waiting up to the
     * specified wait time if necessary for an element to become available.
     *
     * @param timeout how long to wait before giving up, in units of
     *        <tt>unit</tt>
     * @param unit a <tt>TimeUnit</tt> determining how to interpret the
     *        <tt>timeout</tt> parameter
     * @return the head of this queue, or <tt>null</tt> if the
     *         specified waiting time elapses before an element is available
     * @throws Exception errors
     */
    public byte[] poll(long timeout, TimeUnit unit) throws Exception
    {
        return internalPoll(timeout, unit);
    }

    /**
     * Attempts to remove the head of the queue and return it. Returns null if the queue is empty.
     *
     * @return Head of the queue or null.
     * @throws Exception errors
     */
    public byte[] poll() throws Exception
    {
        try
        {
            return remove();
        }
        catch ( NoSuchElementException e )
        {
            return null;
        }
    }

    protected void ensurePath() throws Exception
    {
        ensureContainers.ensure();
    }

    private byte[] internalPoll(long timeout, TimeUnit unit) throws Exception
    {
        ensurePath();

        long            startMs = System.currentTimeMillis();
        boolean         hasTimeout = (unit != null);
        long            maxWaitMs = hasTimeout ? TimeUnit.MILLISECONDS.convert(timeout, unit) : Long.MAX_VALUE;
        for(;;)
        {
            final CountDownLatch    latch = new CountDownLatch(1);
            Watcher                 watcher = new Watcher()
            {
                @Override
                public void process(WatchedEvent event)
                {
                    latch.countDown();
                }
            };
            byte[]      bytes;
            try
            {
                bytes = internalElement(true, watcher);
            }
            catch ( NoSuchElementException dummy )
            {
                log.debug("Parent containers appear to have lapsed - recreate and retry");
                ensureContainers.reset();
                continue;
            }
            if ( bytes != null )
            {
                return bytes;
            }

            if ( hasTimeout )
            {
                long        elapsedMs = System.currentTimeMillis() - startMs;
                long        thisWaitMs = maxWaitMs - elapsedMs;
                if ( thisWaitMs <= 0 )
                {
                    return null;
                }
                latch.await(thisWaitMs, TimeUnit.MILLISECONDS);
            }
            else
            {
                latch.await();
            }
        }
    }

    private byte[] internalElement(boolean removeIt, Watcher watcher) throws Exception
    {
        ensurePath();

        List<String> nodes;
        try
        {
            nodes = (watcher != null) ? client.getChildren().usingWatcher(watcher).forPath(path) : client.getChildren().forPath(path);
        }
        catch ( KeeperException.NoNodeException dummy )
        {
            throw new NoSuchElementException();
        }
        Collections.sort(nodes);

        for ( String node : nodes )
        {
            if ( !node.startsWith(PREFIX) )
            {
                log.warn("Foreign node in queue path: " + node);
                continue;
            }

            String  thisPath = ZKPaths.makePath(path, node);
            try
            {
                byte[] bytes = client.getData().forPath(thisPath);
                if ( removeIt )
                {
                    client.delete().forPath(thisPath);
                }
                return bytes;
            }
            catch ( KeeperException.NoNodeException ignore )
            {
                //Another client removed the node first, try next
            }
        }

        return null;
    }
}
