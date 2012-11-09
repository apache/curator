/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.curator.framework.recipes.nodes;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.ACLBackgroundPathAndBytesable;
import com.netflix.curator.framework.api.BackgroundCallback;
import com.netflix.curator.framework.api.CreateModable;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.PathAndBytesable;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;
import com.netflix.curator.utils.EnsurePath;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>
 *     A persistent ephemeral node is an ephemeral node that attempts to stay present in ZooKeeper, even through connection and session interruptions.
 * </p>
 *
 * <p>
 *     Thanks to bbeck (https://github.com/bbeck) for the initial coding and design
 * </p>
 */
public class PersistentEphemeralNode implements Closeable
{
    private final Logger                    log = LoggerFactory.getLogger(getClass());
    private final CuratorFramework          client;
    private final EnsurePath                ensurePath;
    private final PathAndBytesable<String>  createMethod;
    private final AtomicReference<String>   nodePath = new AtomicReference<String>(null);
    private final String                    basePath;
    private final byte[]                    data;
    private final AtomicReference<State>    state = new AtomicReference<State>(State.LATENT);
    private volatile CountDownLatch         initialCreateLatch = new CountDownLatch(1);
    private final Watcher                   watcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            createNode();
        }
    };
    private final ConnectionStateListener   listener = new ConnectionStateListener()
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
    private final BackgroundCallback        checkExistsCallback = new BackgroundCallback()
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
            protected CreateMode getCreateMode()
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
            protected CreateMode getCreateMode()
            {
                return CreateMode.EPHEMERAL_SEQUENTIAL;
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
            protected CreateMode getCreateMode()
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
            protected CreateMode getCreateMode()
            {
                return CreateMode.EPHEMERAL_SEQUENTIAL;
            }

            @Override
            protected boolean isProtected()
            {
                return true;
            }
        }

        ;

        protected abstract CreateMode getCreateMode();

        protected abstract boolean isProtected();
    }

    /**
     * @param curator curator instance
     * @param basePath the base path for the node
     * @param data data for the node
     * @param mode creation/protection mode
     */
    public PersistentEphemeralNode(CuratorFramework curator, String basePath, byte[] data, Mode mode)
    {
        client = Preconditions.checkNotNull(curator, "curator cannot be null");
        this.basePath = Preconditions.checkNotNull(basePath, "basePath cannot be null");
        mode = Preconditions.checkNotNull(mode, "mode cannot be null");
        data = Preconditions.checkNotNull(data, "data cannot be null");

        String parentDir = ZKPaths.getPathAndNode(basePath).getPath();
        ensurePath = curator.newNamespaceAwareEnsurePath(parentDir);

        BackgroundCallback        backgroundCallback = new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
            {
                if ( (event.getResultCode() == KeeperException.Code.OK.intValue()) || (event.getResultCode() == KeeperException.Code.NODEEXISTS.intValue()) )
                {
                    nodePath.set(event.getPath());
                    watchNode();

                    CountDownLatch      localLatch = initialCreateLatch;
                    initialCreateLatch = null;
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

        createMethod = makeCreateMethod(curator, mode, backgroundCallback);
        this.data = Arrays.copyOf(data, data.length);
    }

    /**
     * You must call start() to initiate the persistent ephemeral node. An attempt to create the node
     * in the background will be started
     */
    public void start()
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");

        client.getConnectionStateListenable().addListener(listener);
        createNode();
    }

    /**
     * Block until the either initial node creation initiated by {@link #start()} succeeds or
     * the timeout elapses.
     *
     * @param timeout the maximum time to wait
     * @param unit time unit
     * @return if the node was created before timeout
     * @throws InterruptedException if the thread is interrupted
     */
    public boolean waitForInitialCreate(long timeout, TimeUnit unit) throws InterruptedException
    {
        Preconditions.checkState(state.get() == State.STARTED, "Not started");

        return initialCreateLatch.await(timeout, unit);
    }

    @Override
    public void close()
    {
        if ( !state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            return;
        }

        client.getConnectionStateListenable().removeListener(listener);

        String          localNodePath = nodePath.getAndSet(null);
        if ( localNodePath != null )
        {
            try
            {
                client.delete().guaranteed().forPath(localNodePath);
            }
            catch ( Exception e )
            {
                log.error("Deleting node on close", e);
            }
        }
    }

    @VisibleForTesting
    String getActualPath() throws ExecutionException, InterruptedException
    {
        return nodePath.get();
    }

    private void createNode()
    {
        if ( state.get() != State.STARTED )
        {
            return;
        }

        try
        {
            ensurePath.ensure(client.getZookeeperClient());
            createMethod.forPath(basePath, data);
        }
        catch ( Exception e )
        {
            log.error("Creating node. BasePath: " + basePath, e);
        }
    }

    private void watchNode()
    {
        if ( state.get() != State.STARTED )
        {
            return;
        }

        String          localNodePath = nodePath.get();
        if ( localNodePath != null )
        {
            try
            {
                client.checkExists().usingWatcher(watcher).inBackground(checkExistsCallback).forPath(localNodePath);
            }
            catch ( Exception e )
            {
                log.error("Watching node: " + localNodePath, e);
            }
        }
    }

    private static PathAndBytesable<String> makeCreateMethod(CuratorFramework curator, Mode mode, BackgroundCallback backgroundCallback)
    {
        CreateModable<ACLBackgroundPathAndBytesable<String>> builder = mode.isProtected() ? curator.create().withProtection() : curator.create();
        return builder.withMode(mode.getCreateMode()).inBackground(backgroundCallback);
    }
}
