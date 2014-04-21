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

package org.apache.curator.framework.recipes.nodes;

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ACLBackgroundPathAndBytesable;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CreateModable;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>
 * A persistent ephemeral node is an ephemeral node that attempts to stay present in
 * ZooKeeper, even through connection and session interruptions.
 * </p>
 * <p>
 * Thanks to bbeck (https://github.com/bbeck) for the initial coding and design
 * </p>
 */
public class PersistentEphemeralNode implements Closeable
{
    private final AtomicReference<CountDownLatch> initialCreateLatch = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorFramework client;
    private final CreateModable<ACLBackgroundPathAndBytesable<String>> createMethod;
    private final AtomicReference<String> nodePath = new AtomicReference<String>(null);
    private final String basePath;
    private final Mode mode;
    private final AtomicReference<byte[]> data = new AtomicReference<byte[]>();
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);
    private final BackgroundCallback backgroundCallback;
    private final Watcher watcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            createNode();
        }
    };
    private final BackgroundCallback checkExistsCallback = new BackgroundCallback()
    {
        @Override
        public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
        {
            if ( event.getResultCode() == KeeperException.Code.NONODE.intValue() )
            {
                createNode();
            }
        }
    };
    private final ConnectionStateListener connectionStateListener = new ConnectionStateListener()
    {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
            if ( newState == ConnectionState.RECONNECTED )
            {
                createNode();
            }
        }
    };

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    /**
     * The mode for node creation
     */
    public enum Mode
    {
        /**
         * Same as {@link CreateMode#EPHEMERAL}
         */
        EPHEMERAL()
            {
                @Override
                protected CreateMode getCreateMode(boolean pathIsSet)
                {
                    return CreateMode.EPHEMERAL;
                }

                @Override
                protected boolean isProtected()
                {
                    return false;
                }
            },

        /**
         * Same as {@link CreateMode#EPHEMERAL_SEQUENTIAL}
         */
        EPHEMERAL_SEQUENTIAL()
            {
                @Override
                protected CreateMode getCreateMode(boolean pathIsSet)
                {
                    return pathIsSet ? CreateMode.EPHEMERAL : CreateMode.EPHEMERAL_SEQUENTIAL;
                }

                @Override
                protected boolean isProtected()
                {
                    return false;
                }
            },

        /**
         * Same as {@link CreateMode#EPHEMERAL} with protection
         */
        PROTECTED_EPHEMERAL()
            {
                @Override
                protected CreateMode getCreateMode(boolean pathIsSet)
                {
                    return CreateMode.EPHEMERAL;
                }

                @Override
                protected boolean isProtected()
                {
                    return true;
                }
            },

        /**
         * Same as {@link CreateMode#EPHEMERAL_SEQUENTIAL} with protection
         */
        PROTECTED_EPHEMERAL_SEQUENTIAL()
            {
                @Override
                protected CreateMode getCreateMode(boolean pathIsSet)
                {
                    return pathIsSet ? CreateMode.EPHEMERAL : CreateMode.EPHEMERAL_SEQUENTIAL;
                }

                @Override
                protected boolean isProtected()
                {
                    return true;
                }
            };

        protected abstract CreateMode getCreateMode(boolean pathIsSet);

        protected abstract boolean isProtected();
    }

    /**
     * @param client   client instance
     * @param mode     creation/protection mode
     * @param basePath the base path for the node
     * @param data     data for the node
     */
    public PersistentEphemeralNode(CuratorFramework client, Mode mode, String basePath, byte[] data)
    {
        this.client = Preconditions.checkNotNull(client, "client cannot be null");
        this.basePath = Preconditions.checkNotNull(basePath, "basePath cannot be null");
        this.mode = Preconditions.checkNotNull(mode, "mode cannot be null");
        data = Preconditions.checkNotNull(data, "data cannot be null");

        backgroundCallback = new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
            {
                String path = null;
                if ( event.getResultCode() == KeeperException.Code.NODEEXISTS.intValue() )
                {
                    path = event.getPath();
                }
                else if ( event.getResultCode() == KeeperException.Code.OK.intValue() )
                {
                    path = event.getName();
                }
                if ( path != null )
                {
                    nodePath.set(path);
                    watchNode();

                    CountDownLatch localLatch = initialCreateLatch.getAndSet(null);
                    if ( localLatch != null )
                    {
                        localLatch.countDown();
                    }
                }
                else
                {
                    createNode();
                }
            }
        };

        createMethod = mode.isProtected() ? client.create().creatingParentsIfNeeded().withProtection() : client.create().creatingParentsIfNeeded();
        this.data.set(Arrays.copyOf(data, data.length));
    }

    /**
     * You must call start() to initiate the persistent ephemeral node. An attempt to create the node
     * in the background will be started
     */
    public void start()
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");

        client.getConnectionStateListenable().addListener(connectionStateListener);
        createNode();
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
    public boolean waitForInitialCreate(long timeout, TimeUnit unit) throws InterruptedException
    {
        Preconditions.checkState(state.get() == State.STARTED, "Not started");

        CountDownLatch localLatch = initialCreateLatch.get();
        return (localLatch == null) || localLatch.await(timeout, unit);
    }

    @Override
    public void close() throws IOException
    {
        if ( !state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            return;
        }

        client.getConnectionStateListenable().removeListener(connectionStateListener);

        try
        {
            deleteNode();
        }
        catch ( Exception e )
        {
            throw new IOException(e);
        }
    }

    /**
     * Returns the currently set path or null if the node does not exist
     *
     * @return node path or null
     */
    public String getActualPath()
    {
        return nodePath.get();
    }

    /**
     * Set data that ephemeral node should set in ZK also writes the data to the node
     *
     * @param data new data value
     * @throws Exception errors
     */
    public void setData(byte[] data) throws Exception
    {
        data = Preconditions.checkNotNull(data, "data cannot be null");
        this.data.set(Arrays.copyOf(data, data.length));
        if ( isActive() )
        {
            client.setData().inBackground().forPath(basePath, this.data.get());
        }
    }

    private void deleteNode() throws Exception
    {
        String localNodePath = nodePath.getAndSet(null);
        if ( localNodePath != null )
        {
            try
            {
                client.delete().guaranteed().forPath(localNodePath);
            }
            catch ( KeeperException.NoNodeException ignore )
            {
                // ignore
            }
            catch ( Exception e )
            {
                log.error("Deleting node: " + localNodePath, e);
                throw e;
            }
        }
    }

    private void createNode()
    {
        if ( !isActive() )
        {
            return;
        }

        try
        {
            String existingPath = nodePath.get();
            String createPath = (existingPath != null) ? existingPath : basePath;
            createMethod.withMode(mode.getCreateMode(existingPath != null)).inBackground(backgroundCallback).forPath(createPath, data.get());
        }
        catch ( Exception e )
        {
            log.error("Creating node. BasePath: " + basePath, e);
            throw new RuntimeException(e);  // should never happen unless there's a programming error - so throw RuntimeException
        }
    }

    private void watchNode() throws Exception
    {
        if ( !isActive() )
        {
            return;
        }

        String localNodePath = nodePath.get();
        if ( localNodePath != null )
        {
            try
            {
                client.checkExists().usingWatcher(watcher).inBackground(checkExistsCallback).forPath(localNodePath);
            }
            catch ( Exception e )
            {
                log.error("Watching node: " + localNodePath, e);
                throw e;
            }
        }
    }

    private boolean isActive()
    {
        return (state.get() == State.STARTED);
    }
}