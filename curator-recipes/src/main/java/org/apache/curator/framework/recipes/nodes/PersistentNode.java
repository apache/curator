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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.WatcherRemoveCuratorFramework;
import org.apache.curator.framework.api.ACLBackgroundPathAndBytesable;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.CreateBuilderMain;
import org.apache.curator.framework.api.CreateModable;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.listen.StandardListenerManager;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>
 * A persistent node is a node that attempts to stay present in
 * ZooKeeper, even through connection and session interruptions.
 * </p>
 * <p>
 * Thanks to bbeck (https://github.com/bbeck) for the initial coding and design
 * </p>
 */
public class PersistentNode implements Closeable
{
    private final AtomicReference<CountDownLatch> initialCreateLatch = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final WatcherRemoveCuratorFramework client;
    private final AtomicReference<String> nodePath = new AtomicReference<String>(null);
    private final String basePath;
    private final CreateMode mode;
    private final long ttl;
    private final AtomicReference<byte[]> data = new AtomicReference<byte[]>();
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);
    private volatile boolean authFailure;
    private volatile boolean parentCreationFailure;
    private final BackgroundCallback backgroundCallback;
    private final boolean useProtection;
    private final boolean useParentCreation;
    private final AtomicReference<CreateModable<ACLBackgroundPathAndBytesable<String>>> createMethod = new AtomicReference<CreateModable<ACLBackgroundPathAndBytesable<String>>>(null);
    private final StandardListenerManager<PersistentNodeListener> listeners = StandardListenerManager.standard();
    private final CuratorWatcher watcher = new CuratorWatcher()
    {
        @Override
        public void process(WatchedEvent event) throws Exception
        {
            if ( isActive() )
            {
                if ( event.getType() == EventType.NodeDeleted )
                {
                    createNode();
                }
                else if ( event.getType() == EventType.NodeDataChanged )
                {
                    watchNode();
                }
            }
        }
    };

    private final BackgroundCallback checkExistsCallback = new BackgroundCallback()
    {
        @Override
        public void processResult(CuratorFramework dummy, CuratorEvent event) throws Exception
        {
            if ( isActive() )
            {
                if ( event.getResultCode() == KeeperException.Code.NONODE.intValue() )
                {
                    createNode();
                }
                else
                {
                    boolean isEphemeral = event.getStat().getEphemeralOwner() != 0;
                    if ( isEphemeral != mode.isEphemeral() )
                    {
                        log.warn("Existing node ephemeral state doesn't match requested state. Maybe the node was created outside of PersistentNode? " + basePath);
                    }
                }
            }
            else
            {
                client.removeWatchers();
            }
        }
    };
    private final BackgroundCallback setDataCallback = new BackgroundCallback()
    {

        @Override
        public void processResult(CuratorFramework dummy, CuratorEvent event)
            throws Exception
        {
            //If the result is ok then initialisation is complete (if we're still initialising)
            //Don't retry on other errors as the only recoverable cases will be connection loss
            //and the node not existing, both of which are already handled by other watches.
            if ( event.getResultCode() == KeeperException.Code.OK.intValue() )
            {
                //Update is ok, mark initialisation as complete if required.
                initialisationComplete();
            }
            else if ( event.getResultCode() == KeeperException.Code.NOAUTH.intValue() )
            {
                log.warn("Client does not have authorisation to write node at path {}", event.getPath());
                authFailure = true;
            }
        }
    };
    private final ConnectionStateListener connectionStateListener = new ConnectionStateListener()
    {
        @Override
        public void stateChanged(CuratorFramework dummy, ConnectionState newState)
        {
            if ( (newState == ConnectionState.RECONNECTED) && isActive() )
            {
                createNode();
            }
        }
    };

    @VisibleForTesting
    volatile CountDownLatch debugCreateNodeLatch = null;

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    /**
     * @param givenClient        client instance
     * @param mode          creation mode
     * @param useProtection if true, call {@link CreateBuilder#withProtection()}
     * @param basePath the base path for the node
     * @param initData data for the node
     */
    public PersistentNode(CuratorFramework givenClient, final CreateMode mode, boolean useProtection, final String basePath, byte[] initData)
    {
        this(givenClient, mode, useProtection, basePath, initData, -1, true);
    }

    /**
     * @param givenClient        client instance
     * @param mode          creation mode
     * @param useProtection if true, call {@link CreateBuilder#withProtection()}
     * @param basePath the base path for the node
     * @param initData data for the node
     * @param useParentCreation if true, call {@link CreateBuilder#creatingParentContainersIfNeeded()}
     */
    public PersistentNode(CuratorFramework givenClient, final CreateMode mode, boolean useProtection, final String basePath, byte[] initData, boolean useParentCreation) {
        this(givenClient, mode, useProtection, basePath, initData, -1, useParentCreation);
    }

    /**
     * @param givenClient        client instance
     * @param mode          creation mode
     * @param useProtection if true, call {@link CreateBuilder#withProtection()}
     * @param basePath the base path for the node
     * @param initData data for the node
     * @param ttl for ttl modes, the ttl to use
     * @param useParentCreation if true, call {@link CreateBuilder#creatingParentContainersIfNeeded()}
     */
    public PersistentNode(CuratorFramework givenClient, final CreateMode mode, boolean useProtection, final String basePath, byte[] initData, long ttl, boolean useParentCreation)
    {
        this.useProtection = useProtection;
        this.useParentCreation = useParentCreation;
        this.client = Preconditions.checkNotNull(givenClient, "client cannot be null").newWatcherRemoveCuratorFramework();
        this.basePath = PathUtils.validatePath(basePath);
        this.mode = Preconditions.checkNotNull(mode, "mode cannot be null");
        this.ttl = ttl;
        final byte[] data = Preconditions.checkNotNull(initData, "data cannot be null");

        backgroundCallback = new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework dummy, CuratorEvent event) throws Exception
            {
                if ( isActive() )
                {
                    processBackgroundCallback(event);
                }
                else
                {
                    processBackgroundCallbackClosedState(event);
                }
            }
        };

        this.data.set(Arrays.copyOf(data, data.length));
    }

    private void processBackgroundCallbackClosedState(CuratorEvent event)
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
            try
            {
                client.delete().guaranteed().inBackground().forPath(path);
            }
            catch ( Exception e )
            {
                log.error("Could not delete node after close", e);
            }
        }
    }

    private void processBackgroundCallback(CuratorEvent event) throws Exception
    {
        String path = null;
        boolean nodeExists = false;
        if ( event.getResultCode() == KeeperException.Code.NODEEXISTS.intValue() )
        {
            path = event.getPath();
            nodeExists = true;
        }
        else if ( event.getResultCode() == KeeperException.Code.OK.intValue() )
        {
            path = event.getName();
        }
        else if ( event.getResultCode() == KeeperException.Code.NOAUTH.intValue() )
        {
            log.warn("Client does not have authorisation to create node at path {}", event.getPath());
            authFailure = true;
            return;
        } else if ( event.getResultCode() == KeeperException.Code.NONODE.intValue() )
        {
            log.warn("Client cannot create parent hierarchy for path {} with useParentCreation set to {}", event.getPath(), useParentCreation);
            parentCreationFailure = true;
            return;
        }
        if ( path != null )
        {
            authFailure = false;
            nodePath.set(path);
            watchNode();

            if ( nodeExists )
            {
                client.setData().inBackground(setDataCallback).forPath(getActualPath(), getData());
            }
            else
            {
                initialisationComplete();
                notifyListeners();
            }
        }
        else
        {
            createNode();
        }
    }

    private void initialisationComplete()
    {
        CountDownLatch localLatch = initialCreateLatch.getAndSet(null);
        if ( localLatch != null )
        {
            localLatch.countDown();
        }
    }

    /**
     * You must call start() to initiate the persistent node. An attempt to create the node
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

    @VisibleForTesting
    final AtomicLong debugWaitMsForBackgroundBeforeClose = new AtomicLong(0);

    @Override
    public void close() throws IOException
    {
        if ( debugWaitMsForBackgroundBeforeClose.get() > 0 )
        {
            try
            {
                Thread.sleep(debugWaitMsForBackgroundBeforeClose.get());
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
            }
        }

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
            ThreadUtils.checkInterrupted(e);
            throw new IOException(e);
        }

        client.removeWatchers();
    }

    /**
     * Returns the listenable
     *
     * @return listenable
     */
    public Listenable<PersistentNodeListener> getListenable()
    {
        return listeners;
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
     * Set data that node should set in ZK also writes the data to the node. NOTE: it
     * is an error to call this method after {@link #start()} but before the initial create
     * has completed. Use {@link #waitForInitialCreate(long, TimeUnit)} to ensure initial
     * creation.
     *
     * @param data new data value
     * @throws Exception errors
     */
    public void setData(byte[] data) throws Exception
    {
        data = Preconditions.checkNotNull(data, "data cannot be null");
        Preconditions.checkState(nodePath.get() != null, "initial create has not been processed. Call waitForInitialCreate() to ensure.");
        Preconditions.checkState(!parentCreationFailure, "Failed to create parent nodes.");
        this.data.set(Arrays.copyOf(data, data.length));
        if ( isActive() )
        {
            client.setData().inBackground(setDataCallback).forPath(getActualPath(), getData());
        }
    }

    /**
     * Return the current value of our data
     *
     * @return our data
     */
    public byte[] getData()
    {
        return this.data.get();
    }

    protected void deleteNode() throws Exception
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
        }
    }

    private void createNode()
    {
        if ( !isActive() )
        {
            return;
        }

        if ( debugCreateNodeLatch != null )
        {
            try
            {
                debugCreateNodeLatch.await();
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                return;
            }
        }

        try
        {
            String existingPath = nodePath.get(), createPath;
            if ( existingPath != null && !useProtection )
            {
                createPath = existingPath;
            }
            else if ( existingPath != null && mode.isSequential() )
            {
                createPath = basePath + ZKPaths.extractSequentialSuffix(existingPath);
            }
            else
            {
                createPath = basePath;
            }

            CreateModable<ACLBackgroundPathAndBytesable<String>> localCreateMethod = createMethod.get();
            if ( localCreateMethod == null )
            {
                CreateBuilderMain createBuilder = mode.isTTL() ? client.create().withTtl(ttl) : client.create();
                CreateModable<ACLBackgroundPathAndBytesable<String>> tempCreateMethod;
                if (useParentCreation) {
                    tempCreateMethod = useProtection ? createBuilder.creatingParentContainersIfNeeded().withProtection() : createBuilder.creatingParentContainersIfNeeded();
                } else {
                    tempCreateMethod = useProtection ? createBuilder.withProtection() : createBuilder;
                }
                createMethod.compareAndSet(null, tempCreateMethod);
                localCreateMethod = createMethod.get();
            }
            localCreateMethod.withMode(getCreateMode(existingPath != null)).inBackground(backgroundCallback).forPath(createPath, data.get());
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new RuntimeException("Creating node. BasePath: " + basePath, e);  // should never happen unless there's a programming error - so throw RuntimeException
        }
    }

    private CreateMode getCreateMode(boolean pathIsSet)
    {
        if ( pathIsSet )
        {
            switch ( mode )
            {
                default:
                {
                    break;
                }

                case EPHEMERAL_SEQUENTIAL:
                {
                    return CreateMode.EPHEMERAL;    // protection case - node already set
                }

                case PERSISTENT_SEQUENTIAL:
                {
                    return CreateMode.PERSISTENT;    // protection case - node already set
                }

                case PERSISTENT_SEQUENTIAL_WITH_TTL:
                {
                    return CreateMode.PERSISTENT_WITH_TTL;    // protection case - node already set
                }
            }
        }
        return mode;
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
            client.checkExists().usingWatcher(watcher).inBackground(checkExistsCallback).forPath(localNodePath);
        }
    }

    private void notifyListeners()
    {
        final String path = getActualPath();
        listeners.forEach(listener -> {
            try
            {
                listener.nodeCreated(path);
            }
            catch ( Exception e )
            {
                ThreadUtils.checkInterrupted(e);
                log.error("From PersistentNode listener", e);
            }
        });
    }

    private boolean isActive()
    {
        return (state.get() == State.STARTED);
    }

    @VisibleForTesting
    boolean isAuthFailure()
    {
        return authFailure;
    }

    @VisibleForTesting
    boolean isParentCreationFailure() {
        return parentCreationFailure;
    }

}
