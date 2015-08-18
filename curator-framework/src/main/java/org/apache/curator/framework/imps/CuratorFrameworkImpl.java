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

package org.apache.curator.framework.imps;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.curator.CuratorConnectionLossException;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.RetryLoop;
import org.apache.curator.TimeTrace;
import org.apache.curator.framework.AuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.WatcherRemoveCuratorFramework;
import org.apache.curator.framework.api.*;
import org.apache.curator.framework.api.transaction.CuratorMultiTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.TransactionOp;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.framework.state.ConnectionStateManager;
import org.apache.curator.utils.DebugUtils;
import org.apache.curator.utils.EnsurePath;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class CuratorFrameworkImpl implements CuratorFramework
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorZookeeperClient client;
    private final ListenerContainer<CuratorListener> listeners;
    private final ListenerContainer<UnhandledErrorListener> unhandledErrorListeners;
    private final ThreadFactory threadFactory;
    private final int maxCloseWaitMs;
    private final BlockingQueue<OperationAndData<?>> backgroundOperations;
    private final NamespaceImpl namespace;
    private final ConnectionStateManager connectionStateManager;
    private final List<AuthInfo> authInfos;
    private final byte[] defaultData;
    private final FailedDeleteManager failedDeleteManager;
    private final FailedRemoveWatchManager failedRemoveWatcherManager;
    private final CompressionProvider compressionProvider;
    private final ACLProvider aclProvider;
    private final NamespaceFacadeCache namespaceFacadeCache;
    private final NamespaceWatcherMap namespaceWatcherMap = new NamespaceWatcherMap(this);
    private final boolean useContainerParentsIfAvailable;

    private volatile ExecutorService executorService;
    private final AtomicBoolean logAsErrorConnectionErrors = new AtomicBoolean(false);

    private static final boolean LOG_ALL_CONNECTION_ISSUES_AS_ERROR_LEVEL = !Boolean.getBoolean(DebugUtils.PROPERTY_LOG_ONLY_FIRST_CONNECTION_ISSUE_AS_ERROR_LEVEL);

    interface DebugBackgroundListener
    {
        void listen(OperationAndData<?> data);
    }

    volatile DebugBackgroundListener debugListener = null;
    @VisibleForTesting
    public volatile UnhandledErrorListener debugUnhandledErrorListener = null;

    private final AtomicReference<CuratorFrameworkState> state;

    public CuratorFrameworkImpl(CuratorFrameworkFactory.Builder builder)
    {
        ZookeeperFactory localZookeeperFactory = makeZookeeperFactory(builder.getZookeeperFactory());
        this.client = new CuratorZookeeperClient(localZookeeperFactory, builder.getEnsembleProvider(), builder.getSessionTimeoutMs(), builder.getConnectionTimeoutMs(), new Watcher()
        {
            @Override
            public void process(WatchedEvent watchedEvent)
            {
                CuratorEvent event = new CuratorEventImpl(CuratorFrameworkImpl.this, CuratorEventType.WATCHED, watchedEvent.getState().getIntValue(), unfixForNamespace(watchedEvent.getPath()), null, null, null, null, null, watchedEvent, null, null);
                processEvent(event);
            }
        }, builder.getRetryPolicy(), builder.canBeReadOnly());

        listeners = new ListenerContainer<CuratorListener>();
        unhandledErrorListeners = new ListenerContainer<UnhandledErrorListener>();
        backgroundOperations = new DelayQueue<OperationAndData<?>>();
        namespace = new NamespaceImpl(this, builder.getNamespace());
        threadFactory = getThreadFactory(builder);
        maxCloseWaitMs = builder.getMaxCloseWaitMs();
        connectionStateManager = new ConnectionStateManager(this, builder.getThreadFactory());
        compressionProvider = builder.getCompressionProvider();
        aclProvider = builder.getAclProvider();
        state = new AtomicReference<CuratorFrameworkState>(CuratorFrameworkState.LATENT);
        useContainerParentsIfAvailable = builder.useContainerParentsIfAvailable();

        byte[] builderDefaultData = builder.getDefaultData();
        defaultData = (builderDefaultData != null) ? Arrays.copyOf(builderDefaultData, builderDefaultData.length) : new byte[0];
        authInfos = buildAuths(builder);

        failedDeleteManager = new FailedDeleteManager(this);
        failedRemoveWatcherManager = new FailedRemoveWatchManager(this);
        namespaceFacadeCache = new NamespaceFacadeCache(this);
    }

    private List<AuthInfo> buildAuths(CuratorFrameworkFactory.Builder builder)
    {
        ImmutableList.Builder<AuthInfo> builder1 = ImmutableList.builder();
        if ( builder.getAuthInfos() != null )
        {
            builder1.addAll(builder.getAuthInfos());
        }
        return builder1.build();
    }

    @Override
    public WatcherRemoveCuratorFramework newWatcherRemoveCuratorFramework()
    {
        return new WatcherRemovalFacade(this);
    }

    private ZookeeperFactory makeZookeeperFactory(final ZookeeperFactory actualZookeeperFactory)
    {
        return new ZookeeperFactory()
        {
            @Override
            public ZooKeeper newZooKeeper(String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly) throws Exception
            {
                ZooKeeper zooKeeper = actualZookeeperFactory.newZooKeeper(connectString, sessionTimeout, watcher, canBeReadOnly);
                for ( AuthInfo auth : authInfos )
                {
                    zooKeeper.addAuthInfo(auth.getScheme(), auth.getAuth());
                }

                return zooKeeper;
            }
        };
    }

    private ThreadFactory getThreadFactory(CuratorFrameworkFactory.Builder builder)
    {
        ThreadFactory threadFactory = builder.getThreadFactory();
        if ( threadFactory == null )
        {
            threadFactory = ThreadUtils.newThreadFactory("Framework");
        }
        return threadFactory;
    }

    protected CuratorFrameworkImpl(CuratorFrameworkImpl parent)
    {
        client = parent.client;
        listeners = parent.listeners;
        unhandledErrorListeners = parent.unhandledErrorListeners;
        threadFactory = parent.threadFactory;
        maxCloseWaitMs = parent.maxCloseWaitMs;
        backgroundOperations = parent.backgroundOperations;
        connectionStateManager = parent.connectionStateManager;
        defaultData = parent.defaultData;
        failedDeleteManager = parent.failedDeleteManager;
        failedRemoveWatcherManager = parent.failedRemoveWatcherManager;
        compressionProvider = parent.compressionProvider;
        aclProvider = parent.aclProvider;
        namespaceFacadeCache = parent.namespaceFacadeCache;
        namespace = new NamespaceImpl(this, null);
        state = parent.state;
        authInfos = parent.authInfos;
        useContainerParentsIfAvailable = parent.useContainerParentsIfAvailable;
    }

    @Override
    public void createContainers(String path) throws Exception
    {
        checkExists().creatingParentContainersIfNeeded().forPath(ZKPaths.makePath(path, "foo"));
    }

    @Override
    public void clearWatcherReferences(Watcher watcher)
    {
        NamespaceWatcher namespaceWatcher = namespaceWatcherMap.remove(watcher);
        if ( namespaceWatcher != null )
        {
            namespaceWatcher.close();
        }
    }

    @Override
    public CuratorFrameworkState getState()
    {
        return state.get();
    }

    @Override
    @Deprecated
    public boolean isStarted()
    {
        return state.get() == CuratorFrameworkState.STARTED;
    }

    @Override
    public boolean blockUntilConnected(int maxWaitTime, TimeUnit units) throws InterruptedException
    {
        return connectionStateManager.blockUntilConnected(maxWaitTime, units);
    }

    @Override
    public void blockUntilConnected() throws InterruptedException
    {
        blockUntilConnected(0, null);
    }

    @Override
    public void start()
    {
        log.info("Starting");
        if ( !state.compareAndSet(CuratorFrameworkState.LATENT, CuratorFrameworkState.STARTED) )
        {
            throw new IllegalStateException("Cannot be started more than once");
        }

        try
        {
            connectionStateManager.start(); // ordering dependency - must be called before client.start()

            final ConnectionStateListener listener = new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    if ( ConnectionState.CONNECTED == newState || ConnectionState.RECONNECTED == newState )
                    {
                        logAsErrorConnectionErrors.set(true);
                    }
                }
            };

            this.getConnectionStateListenable().addListener(listener);

            client.start();

            executorService = Executors.newFixedThreadPool(2, threadFactory);  // 1 for listeners, 1 for background ops

            executorService.submit(new Callable<Object>()
            {
                @Override
                public Object call() throws Exception
                {
                    backgroundOperationsLoop();
                    return null;
                }
            });
        }
        catch ( Exception e )
        {
            handleBackgroundOperationException(null, e);
        }
    }

    @Override
    public void close()
    {
        log.debug("Closing");
        if ( state.compareAndSet(CuratorFrameworkState.STARTED, CuratorFrameworkState.STOPPED) )
        {
            listeners.forEach(new Function<CuratorListener, Void>()
            {
                @Override
                public Void apply(CuratorListener listener)
                {
                    CuratorEvent event = new CuratorEventImpl(CuratorFrameworkImpl.this, CuratorEventType.CLOSING, 0, null, null, null, null, null, null, null, null, null);
                    try
                    {
                        listener.eventReceived(CuratorFrameworkImpl.this, event);
                    }
                    catch ( Exception e )
                    {
                        log.error("Exception while sending Closing event", e);
                    }
                    return null;
                }
            });

            if ( executorService != null )
            {
                executorService.shutdownNow();
                try
                {
                    executorService.awaitTermination(maxCloseWaitMs, TimeUnit.MILLISECONDS);
                }
                catch ( InterruptedException e )
                {
                    // Interrupted while interrupting; I give up.
                    Thread.currentThread().interrupt();
                }
            }

            listeners.clear();
            unhandledErrorListeners.clear();
            connectionStateManager.close();
            client.close();
            namespaceWatcherMap.close();
        }
    }

    @Override
    @Deprecated
    public CuratorFramework nonNamespaceView()
    {
        return usingNamespace(null);
    }

    @Override
    public String getNamespace()
    {
        String str = namespace.getNamespace();
        return (str != null) ? str : "";
    }

    @Override
    public CuratorFramework usingNamespace(String newNamespace)
    {
        Preconditions.checkState(getState() == CuratorFrameworkState.STARTED, "instance must be started before calling this method");

        return namespaceFacadeCache.get(newNamespace);
    }

    @Override
    public CreateBuilder create()
    {
        Preconditions.checkState(getState() == CuratorFrameworkState.STARTED, "instance must be started before calling this method");

        return new CreateBuilderImpl(this);
    }

    @Override
    public DeleteBuilder delete()
    {
        Preconditions.checkState(getState() == CuratorFrameworkState.STARTED, "instance must be started before calling this method");

        return new DeleteBuilderImpl(this);
    }

    @Override
    public ExistsBuilder checkExists()
    {
        Preconditions.checkState(getState() == CuratorFrameworkState.STARTED, "instance must be started before calling this method");

        return new ExistsBuilderImpl(this);
    }

    @Override
    public GetDataBuilder getData()
    {
        Preconditions.checkState(getState() == CuratorFrameworkState.STARTED, "instance must be started before calling this method");

        return new GetDataBuilderImpl(this);
    }

    @Override
    public SetDataBuilder setData()
    {
        Preconditions.checkState(getState() == CuratorFrameworkState.STARTED, "instance must be started before calling this method");

        return new SetDataBuilderImpl(this);
    }

    @Override
    public GetChildrenBuilder getChildren()
    {
        Preconditions.checkState(getState() == CuratorFrameworkState.STARTED, "instance must be started before calling this method");

        return new GetChildrenBuilderImpl(this);
    }

    @Override
    public GetACLBuilder getACL()
    {
        Preconditions.checkState(getState() == CuratorFrameworkState.STARTED, "instance must be started before calling this method");

        return new GetACLBuilderImpl(this);
    }

    @Override
    public SetACLBuilder setACL()
    {
        Preconditions.checkState(getState() == CuratorFrameworkState.STARTED, "instance must be started before calling this method");

        return new SetACLBuilderImpl(this);
    }

    @Override
    public ReconfigBuilder reconfig()
    {
        return new ReconfigBuilderImpl(this);
    }

    @Override
    public GetConfigBuilder getConfig()
    {
        return new GetConfigBuilderImpl(this);
    }

    @Override
    public CuratorTransaction inTransaction()
    {
        Preconditions.checkState(getState() == CuratorFrameworkState.STARTED, "instance must be started before calling this method");

        return new CuratorTransactionImpl(this);
    }

    @Override
    public CuratorMultiTransaction transaction()
    {
        Preconditions.checkState(getState() == CuratorFrameworkState.STARTED, "instance must be started before calling this method");

        return new CuratorMultiTransactionImpl(this);
    }

    @Override
    public TransactionOp transactionOp()
    {
        Preconditions.checkState(getState() == CuratorFrameworkState.STARTED, "instance must be started before calling this method");

        return new TransactionOpImpl(this);
    }

    @Override
    public Listenable<ConnectionStateListener> getConnectionStateListenable()
    {
        return connectionStateManager.getListenable();
    }

    @Override
    public Listenable<CuratorListener> getCuratorListenable()
    {
        return listeners;
    }

    @Override
    public Listenable<UnhandledErrorListener> getUnhandledErrorListenable()
    {
        return unhandledErrorListeners;
    }

    @Override
    public void sync(String path, Object context)
    {
        Preconditions.checkState(getState() == CuratorFrameworkState.STARTED, "instance must be started before calling this method");

        path = fixForNamespace(path);

        internalSync(this, path, context);
    }

    @Override
    public SyncBuilder sync()
    {
        return new SyncBuilderImpl(this);
    }

    @Override
    public RemoveWatchesBuilder watches()
    {
        return new RemoveWatchesBuilderImpl(this);
    }

    protected void internalSync(CuratorFrameworkImpl impl, String path, Object context)
    {
        BackgroundOperation<String> operation = new BackgroundSyncImpl(impl, context);
        performBackgroundOperation(new OperationAndData<String>(operation, path, null, null, context));
    }

    @Override
    public CuratorZookeeperClient getZookeeperClient()
    {
        return client;
    }

    @Override
    public EnsurePath newNamespaceAwareEnsurePath(String path)
    {
        return namespace.newNamespaceAwareEnsurePath(path);
    }

    ACLProvider getAclProvider()
    {
        return aclProvider;
    }

    FailedDeleteManager getFailedDeleteManager()
    {
        return failedDeleteManager;
    }

    FailedRemoveWatchManager getFailedRemoveWatcherManager()
    {
        return failedRemoveWatcherManager;
    }

    RetryLoop newRetryLoop()
    {
        return client.newRetryLoop();
    }

    ZooKeeper getZooKeeper() throws Exception
    {
        return client.getZooKeeper();
    }

    CompressionProvider getCompressionProvider()
    {
        return compressionProvider;
    }

    boolean useContainerParentsIfAvailable()
    {
        return useContainerParentsIfAvailable;
    }

    <DATA_TYPE> void processBackgroundOperation(OperationAndData<DATA_TYPE> operationAndData, CuratorEvent event)
    {
        boolean isInitialExecution = (event == null);
        if ( isInitialExecution )
        {
            performBackgroundOperation(operationAndData);
            return;
        }

        boolean doQueueOperation = false;
        do
        {
            if ( RetryLoop.shouldRetry(event.getResultCode()) )
            {
                doQueueOperation = checkBackgroundRetry(operationAndData, event);
                break;
            }

            if ( operationAndData.getCallback() != null )
            {
                sendToBackgroundCallback(operationAndData, event);
                break;
            }

            processEvent(event);
        }
        while ( false );

        if ( doQueueOperation )
        {
            queueOperation(operationAndData);
        }
    }

    <DATA_TYPE> void queueOperation(OperationAndData<DATA_TYPE> operationAndData)
    {
        backgroundOperations.offer(operationAndData);
    }

    void logError(String reason, final Throwable e)
    {
        if ( (reason == null) || (reason.length() == 0) )
        {
            reason = "n/a";
        }

        if ( !Boolean.getBoolean(DebugUtils.PROPERTY_DONT_LOG_CONNECTION_ISSUES) || !(e instanceof KeeperException) )
        {
            if ( e instanceof KeeperException.ConnectionLossException )
            {
                if ( LOG_ALL_CONNECTION_ISSUES_AS_ERROR_LEVEL || logAsErrorConnectionErrors.compareAndSet(true, false) )
                {
                    log.error(reason, e);
                }
                else
                {
                    log.debug(reason, e);
                }
            }
            else
            {
                log.error(reason, e);
            }
        }

        final String localReason = reason;
        unhandledErrorListeners.forEach(new Function<UnhandledErrorListener, Void>()
        {
            @Override
            public Void apply(UnhandledErrorListener listener)
            {
                listener.unhandledError(localReason, e);
                return null;
            }
        });

        if ( debugUnhandledErrorListener != null )
        {
            debugUnhandledErrorListener.unhandledError(reason, e);
        }
    }

    String unfixForNamespace(String path)
    {
        return namespace.unfixForNamespace(path);
    }

    String fixForNamespace(String path)
    {
        return namespace.fixForNamespace(path, false);
    }

    String fixForNamespace(String path, boolean isSequential)
    {
        return namespace.fixForNamespace(path, isSequential);
    }

    byte[] getDefaultData()
    {
        return defaultData;
    }

    NamespaceFacadeCache getNamespaceFacadeCache()
    {
        return namespaceFacadeCache;
    }

    NamespaceWatcherMap getNamespaceWatcherMap()
    {
        return namespaceWatcherMap;
    }

    void validateConnection(Watcher.Event.KeeperState state)
    {
        if ( state == Watcher.Event.KeeperState.Disconnected )
        {
            suspendConnection();
        }
        else if ( state == Watcher.Event.KeeperState.Expired )
        {
            connectionStateManager.addStateChange(ConnectionState.LOST);
        }
        else if ( state == Watcher.Event.KeeperState.SyncConnected )
        {
            connectionStateManager.addStateChange(ConnectionState.RECONNECTED);
        }
        else if ( state == Watcher.Event.KeeperState.ConnectedReadOnly )
        {
            connectionStateManager.addStateChange(ConnectionState.READ_ONLY);
        }
    }

    Watcher.Event.KeeperState codeToState(KeeperException.Code code)
    {
        switch ( code )
        {
        case AUTHFAILED:
        case NOAUTH:
        {
            return Watcher.Event.KeeperState.AuthFailed;
        }

        case CONNECTIONLOSS:
        case OPERATIONTIMEOUT:
        {
            return Watcher.Event.KeeperState.Disconnected;
        }

        case SESSIONEXPIRED:
        {
            return Watcher.Event.KeeperState.Expired;
        }

        case OK:
        case SESSIONMOVED:
        {
            return Watcher.Event.KeeperState.SyncConnected;
        }
        }
        return Watcher.Event.KeeperState.fromInt(-1);
    }

    WatcherRemovalManager getWatcherRemovalManager()
    {
        return null;
    }

    private void suspendConnection()
    {
        if ( !connectionStateManager.setToSuspended() )
        {
            return;
        }

        doSyncForSuspendedConnection(client.getInstanceIndex());
    }

    private void doSyncForSuspendedConnection(final long instanceIndex)
    {
        // we appear to have disconnected, force a new ZK event and see if we can connect to another server
        final BackgroundOperation<String> operation = new BackgroundSyncImpl(this, null);
        OperationAndData.ErrorCallback<String> errorCallback = new OperationAndData.ErrorCallback<String>()
        {
            @Override
            public void retriesExhausted(OperationAndData<String> operationAndData)
            {
                // if instanceIndex != newInstanceIndex, the ZooKeeper instance was reset/reallocated
                // so the pending background sync is no longer valid.
                // if instanceIndex is -1, this is the second try to sync - punt and mark the connection lost
                if ( (instanceIndex < 0) || (instanceIndex == client.getInstanceIndex()) )
                {
                    connectionStateManager.addStateChange(ConnectionState.LOST);
                }
                else
                {
                    log.debug("suspendConnection() failure ignored as the ZooKeeper instance was reset. Retrying.");
                    // send -1 to signal that if it happens again, punt and mark the connection lost
                    doSyncForSuspendedConnection(-1);
                }
            }
        };
        performBackgroundOperation(new OperationAndData<String>(operation, "/", null, errorCallback, null));
    }

    @SuppressWarnings({"ThrowableResultOfMethodCallIgnored"})
    private <DATA_TYPE> boolean checkBackgroundRetry(OperationAndData<DATA_TYPE> operationAndData, CuratorEvent event)
    {
        boolean doRetry = false;
        if ( client.getRetryPolicy().allowRetry(operationAndData.getThenIncrementRetryCount(), operationAndData.getElapsedTimeMs(), operationAndData) )
        {
            doRetry = true;
        }
        else
        {
            if ( operationAndData.getErrorCallback() != null )
            {
                operationAndData.getErrorCallback().retriesExhausted(operationAndData);
            }

            if ( operationAndData.getCallback() != null )
            {
                sendToBackgroundCallback(operationAndData, event);
            }

            KeeperException.Code code = KeeperException.Code.get(event.getResultCode());
            Exception e = null;
            try
            {
                e = (code != null) ? KeeperException.create(code) : null;
            }
            catch ( Throwable ignore )
            {
            }
            if ( e == null )
            {
                e = new Exception("Unknown result codegetResultCode()");
            }

            validateConnection(codeToState(code));
            logError("Background operation retry gave up", e);
        }
        return doRetry;
    }

    private <DATA_TYPE> void sendToBackgroundCallback(OperationAndData<DATA_TYPE> operationAndData, CuratorEvent event)
    {
        try
        {
            operationAndData.getCallback().processResult(this, event);
        }
        catch ( Exception e )
        {
            handleBackgroundOperationException(operationAndData, e);
        }
    }

    private <DATA_TYPE> void handleBackgroundOperationException(OperationAndData<DATA_TYPE> operationAndData, Throwable e)
    {
        do
        {
            if ( (operationAndData != null) && RetryLoop.isRetryException(e) )
            {
                if ( !Boolean.getBoolean(DebugUtils.PROPERTY_DONT_LOG_CONNECTION_ISSUES) )
                {
                    log.debug("Retry-able exception received", e);
                }
                if ( client.getRetryPolicy().allowRetry(operationAndData.getThenIncrementRetryCount(), operationAndData.getElapsedTimeMs(), operationAndData) )
                {
                    if ( !Boolean.getBoolean(DebugUtils.PROPERTY_DONT_LOG_CONNECTION_ISSUES) )
                    {
                        log.debug("Retrying operation");
                    }
                    backgroundOperations.offer(operationAndData);
                    break;
                }
                else
                {
                    if ( !Boolean.getBoolean(DebugUtils.PROPERTY_DONT_LOG_CONNECTION_ISSUES) )
                    {
                        log.debug("Retry policy did not allow retry");
                    }
                    if ( operationAndData.getErrorCallback() != null )
                    {
                        operationAndData.getErrorCallback().retriesExhausted(operationAndData);
                    }
                }
            }

            logError("Background exception was not retry-able or retry gave up", e);
        }
        while ( false );
    }

    private void backgroundOperationsLoop()
    {
        while ( !Thread.currentThread().isInterrupted() )
        {
            OperationAndData<?> operationAndData;
            try
            {
                operationAndData = backgroundOperations.take();
                if ( debugListener != null )
                {
                    debugListener.listen(operationAndData);
                }
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                break;
            }

            performBackgroundOperation(operationAndData);
        }
    }

    private void performBackgroundOperation(OperationAndData<?> operationAndData)
    {
        try
        {
            if ( !operationAndData.isConnectionRequired() || client.isConnected() )
            {
                operationAndData.callPerformBackgroundOperation();
            }
            else
            {
                client.getZooKeeper();  // important - allow connection resets, timeouts, etc. to occur
                if ( operationAndData.getElapsedTimeMs() >= client.getConnectionTimeoutMs() )
                {
                    throw new CuratorConnectionLossException();
                }
                operationAndData.sleepFor(1, TimeUnit.SECONDS);
                queueOperation(operationAndData);
            }
        }
        catch ( Throwable e )
        {
            /**
             * Fix edge case reported as CURATOR-52. ConnectionState.checkTimeouts() throws KeeperException.ConnectionLossException
             * when the initial (or previously failed) connection cannot be re-established. This needs to be run through the retry policy
             * and callbacks need to get invoked, etc.
             */
            if ( e instanceof CuratorConnectionLossException )
            {
                WatchedEvent watchedEvent = new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.Disconnected, null);
                CuratorEvent event = new CuratorEventImpl(this, CuratorEventType.WATCHED, KeeperException.Code.CONNECTIONLOSS.intValue(), null, null, operationAndData.getContext(), null, null, null, watchedEvent, null, null);
                if ( checkBackgroundRetry(operationAndData, event) )
                {
                    queueOperation(operationAndData);
                }
                else
                {
                    logError("Background retry gave up", e);
                }
            }
            else
            {
                handleBackgroundOperationException(operationAndData, e);
            }
        }
    }

    private void processEvent(final CuratorEvent curatorEvent)
    {
        if ( curatorEvent.getType() == CuratorEventType.WATCHED )
        {
            validateConnection(curatorEvent.getWatchedEvent().getState());
        }

        listeners.forEach(new Function<CuratorListener, Void>()
        {
            @Override
            public Void apply(CuratorListener listener)
            {
                try
                {
                    TimeTrace trace = client.startTracer("EventListener");
                    listener.eventReceived(CuratorFrameworkImpl.this, curatorEvent);
                    trace.commit();
                }
                catch ( Exception e )
                {
                    logError("Event listener threw exception", e);
                }
                return null;
            }
        });
    }
}
