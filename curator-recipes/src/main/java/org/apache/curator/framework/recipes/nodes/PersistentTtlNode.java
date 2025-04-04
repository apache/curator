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

package org.apache.curator.framework.recipes.nodes;

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 *     Manages a {@link PersistentNode} that uses {@link CreateMode#PERSISTENT_WITH_TTL}. Asynchronously
 *     it creates or updates a child on the persistent node that is marked with a provided TTL.
 * </p>
 *
 * <p>
 *     The effect of this is to have a node that can be watched, etc. The child node serves as
 *     a method of having the parent node deleted if the TTL expires. i.e. if the process
 *     that is running the PersistentTtlNode crashes and the TTL elapses, first the child node
 *     will be deleted due to the TTL expiration and then the parent node will be deleted as it's
 *     a TTL node with no children.
 * </p>
 *
 * <p>
 *     PersistentTtlNode is useful when you need to create a TTL node but don't want to keep
 *     it alive manually by periodically setting data - PersistentTtlNode does that for you. Further
 *     the keep-alive is done in a way that does not generate watch triggers on the parent node.
 * </p>
 */
public class PersistentTtlNode implements Closeable {
    public static final String DEFAULT_CHILD_NODE_NAME = "touch";
    public static final int DEFAULT_TOUCH_SCHEDULE_FACTOR = 2;
    public static final boolean DEFAULT_USE_PARENT_CREATION = true;

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final PersistentNode node;
    private final CuratorFramework client;
    private final long ttlMs;
    private final int touchScheduleFactor;
    private final ScheduledExecutorService executorService;
    private final AtomicReference<Future<?>> futureRef = new AtomicReference<>();
    private final String childPath;

    /**
     * @param client the client
     * @param path path for the parent ZNode
     * @param ttlMs max ttl for the node in milliseconds
     * @param initData data for the node
     */
    public PersistentTtlNode(CuratorFramework client, String path, long ttlMs, byte[] initData) {
        this(
                client,
                Executors.newSingleThreadScheduledExecutor(ThreadUtils.newThreadFactory("PersistentTtlNode")),
                path,
                ttlMs,
                initData,
                DEFAULT_CHILD_NODE_NAME,
                DEFAULT_TOUCH_SCHEDULE_FACTOR,
                DEFAULT_USE_PARENT_CREATION);
    }

    /**
     * @param client the client
     * @param path path for the parent ZNode
     * @param ttlMs max ttl for the node in milliseconds
     * @param initData data for the node
     * @param useParentCreation if true, parent ZNode can be created without ancestors
     */
    public PersistentTtlNode(
            CuratorFramework client, String path, long ttlMs, byte[] initData, boolean useParentCreation) {
        this(
                client,
                Executors.newSingleThreadScheduledExecutor(ThreadUtils.newThreadFactory("PersistentTtlNode")),
                path,
                ttlMs,
                initData,
                DEFAULT_CHILD_NODE_NAME,
                DEFAULT_TOUCH_SCHEDULE_FACTOR,
                useParentCreation);
    }

    /**
     * @param client the client
     * @param executorService  ExecutorService to use for background thread. This service should be single threaded, otherwise you may see inconsistent results.
     * @param path path for the parent ZNode
     * @param ttlMs max ttl for the node in milliseconds
     * @param initData data for the node
     * @param childNodeName name to use for the child node of the node created at <code>path</code>
     * @param touchScheduleFactor how ofter to set/create the child node as a factor of the ttlMs. i.e.
     *                            the child is touched every <code>(ttlMs / touchScheduleFactor)</code>
     */
    public PersistentTtlNode(
            CuratorFramework client,
            ScheduledExecutorService executorService,
            String path,
            long ttlMs,
            byte[] initData,
            String childNodeName,
            int touchScheduleFactor) {
        this(
                client,
                executorService,
                path,
                ttlMs,
                initData,
                childNodeName,
                touchScheduleFactor,
                DEFAULT_USE_PARENT_CREATION);
    }

    /**
     * @param client the client
     * @param executorService  ExecutorService to use for background thread. This service should be single threaded, otherwise you may see inconsistent results.
     * @param path path for the parent ZNode
     * @param ttlMs max ttl for the node in milliseconds
     * @param initData data for the node
     * @param childNodeName name to use for the child node of the node created at <code>path</code>
     * @param touchScheduleFactor how ofter to set/create the child node as a factor of the ttlMs. i.e.
     *                            the child is touched every <code>(ttlMs / touchScheduleFactor)</code>
     * @param useParentCreation if true, parent ZNode can be created without ancestors
     */
    public PersistentTtlNode(
            CuratorFramework client,
            ScheduledExecutorService executorService,
            String path,
            long ttlMs,
            byte[] initData,
            String childNodeName,
            int touchScheduleFactor,
            boolean useParentCreation) {
        this.client = Objects.requireNonNull(client, "client cannot be null");
        this.ttlMs = ttlMs;
        this.touchScheduleFactor = touchScheduleFactor;
        node =
                new PersistentNode(
                        client, CreateMode.PERSISTENT_WITH_TTL, false, path, initData, ttlMs, useParentCreation) {
                    @Override
                    protected void deleteNode() {
                        // NOP
                    }
                };
        this.executorService = Objects.requireNonNull(executorService, "executorService cannot be null");
        childPath = ZKPaths.makePath(Objects.requireNonNull(path, "path cannot be null"), childNodeName);
    }

    @VisibleForTesting
    void touch() {
        try {
            try {
                client.setData().forPath(childPath);
            } catch (KeeperException.NoNodeException e) {
                client.create()
                        .orSetData()
                        .withTtl(ttlMs)
                        .withMode(CreateMode.PERSISTENT_WITH_TTL)
                        .forPath(childPath);
            }
        } catch (KeeperException.NoNodeException ignore) {
            // ignore
        } catch (Exception e) {
            if (!ThreadUtils.checkInterrupted(e)) {
                log.debug("Could not touch child node", e);
            }
        }
    }

    /**
     * You must call start() to initiate the persistent ttl node
     */
    public void start() {
        node.start();
        final Runnable touchTask = () -> touch();
        Future<?> future = executorService.scheduleAtFixedRate(
        		touchTask, ttlMs / touchScheduleFactor, ttlMs / touchScheduleFactor, TimeUnit.MILLISECONDS);
        futureRef.set(future);
    }

    /**
     * Block until the either initial node creation initiated by {@link #start()} succeeds or
     * the timeout elapses.
     *
     * @param timeout the maximum time to wait
     * @param unit    time unit
     * @return if the node was created before timeout
     * @throws InterruptedException if the thread is interrupted
     */
    public boolean waitForInitialCreate(long timeout, TimeUnit unit) throws InterruptedException {
        return node.waitForInitialCreate(timeout, unit);
    }

    /**
     * Set data that node should set in ZK also writes the data to the node. NOTE: it
     * is an error to call this method after {@link #start()} but before the initial create
     * has completed. Use {@link #waitForInitialCreate(long, TimeUnit)} to ensure initial
     * creation.
     *
     * @param data new data value
     * @throws Exception errors
     */
    public void setData(byte[] data) throws Exception {
        node.setData(data);
    }

    /**
     * Return the current value of our data
     *
     * @return our data
     */
    public byte[] getData() {
        return node.getData();
    }

    /**
     * Call when you are done with the PersistentTtlNode. Note: the ZNode is <em>not</em> immediately
     * deleted. However, if no other PersistentTtlNode with the same path is running the node will get deleted
     * based on the ttl.
     */
    @Override
    public void close() {
        Future<?> future = futureRef.getAndSet(null);
        if (future != null) {
            future.cancel(true);
        }
        try {
            node.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
