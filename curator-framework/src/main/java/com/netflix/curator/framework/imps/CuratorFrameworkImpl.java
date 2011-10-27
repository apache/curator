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
package com.netflix.curator.framework.imps;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import com.netflix.curator.CuratorZookeeperClient;
import com.netflix.curator.RetryLoop;
import com.netflix.curator.TimeTrace;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.api.*;
import com.netflix.curator.utils.EnsurePath;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

public class CuratorFrameworkImpl implements CuratorFramework
{
    private final CuratorZookeeperClient                                client;
    private final Map<CuratorListener, ListenerEntry<CuratorListener>>  listeners;
    private final ExecutorService                                       executorService;
    private final BlockingQueue<OperationAndData<?>>                    backgroundOperations;
    private final String                                                namespace;
    private final EnsurePath                                            ensurePath;
    private final AtomicReference<AuthInfo>                             authInfo = new AtomicReference<AuthInfo>();

    private enum State
    {
        LATENT,
        STARTED,
        STOPPED
    }
    private final AtomicReference<State>                    state = new AtomicReference<State>(State.LATENT);

    private static class AuthInfo
    {
        final String    scheme;
        final byte[]    auth;

        private AuthInfo(String scheme, byte[] auth)
        {
            this.scheme = scheme;
            this.auth = auth;
        }
    }

    public CuratorFrameworkImpl(CuratorFrameworkFactory.Builder builder) throws IOException
    {
        Preconditions.checkNotNull(builder.getThreadFactory());

        this.client = new CuratorZookeeperClient
        (
            builder.getConnectString(),
            builder.getSessionTimeoutMs(),
            builder.getConnectionTimeoutMs(),
            new Watcher()
            {
                @Override
                public void process(WatchedEvent watchedEvent)
                {
                    CuratorEvent event = new CuratorEventImpl
                    (
                        CuratorEventType.WATCHED,
                        watchedEvent.getState().getIntValue(),
                        watchedEvent.getPath(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        watchedEvent,
                        null
                    );
                    processEvent(event);
                }
            },
            builder.getRetryPolicy()
        );

        listeners = Maps.newConcurrentMap();
        backgroundOperations = new LinkedBlockingQueue<OperationAndData<?>>();
        namespace = builder.getNamespace();
        ensurePath = (namespace != null) ? new EnsurePath(ZKPaths.makePath("/", namespace)) : null;
        executorService = Executors.newFixedThreadPool(2, builder.getThreadFactory());  // 1 for listeners, 1 for background ops

        if ( builder.getAuthScheme() != null )
        {
            authInfo.set(new AuthInfo(builder.getAuthScheme(), builder.getAuthValue()));
        }
    }

    protected CuratorFrameworkImpl(CuratorFrameworkImpl parent)
    {
        client = parent.client;
        listeners = parent.listeners;
        executorService = parent.executorService;
        backgroundOperations = parent.backgroundOperations;
        namespace = null;
        ensurePath = null;
    }

    @Override
    public boolean isStarted()
    {
        return state.get() == State.STARTED;
    }

    @Override
    public void     start()
    {
        client.getLog().info("Starting");
        if ( !state.compareAndSet(State.LATENT, State.STARTED) )
        {
            IllegalStateException error = new IllegalStateException();
            client.getLog().error("Already started", error);
            throw error;
        }

        try
        {
            client.start();

            executorService.submit
            (
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        Thread.currentThread().setName("CuratorFrameworkImpl " + Thread.currentThread().getName());
                        backgroundOperationsLoop();
                        return null;
                    }
                }
            );
        }
        catch ( Exception e )
        {
            handleBackgroundOperationException(null, e);
        }
    }

    @Override
    public void     close()
    {
        client.getLog().debug("Closing");
        if ( !state.compareAndSet(State.STARTED, State.STOPPED) )
        {
            IllegalStateException error = new IllegalStateException();
            client.getLog().error("Already closed", error);
            throw error;
        }

        for ( final ListenerEntry<CuratorListener> entry : listeners.values() )
        {
            entry.executor.execute
            (
                new Runnable()
                {
                    @Override
                    public void run()
                    {
                        CuratorEvent event = new CuratorEventImpl(CuratorEventType.CLOSING, 0, null, null, null, null, null, null, null, null);
                        try
                        {
                            entry.listener.eventReceived(CuratorFrameworkImpl.this, event);
                        }
                        catch ( Exception e )
                        {
                            client.getLog().error("Exception while sending Closing event", e);
                        }
                    }
                }
            );
        }

        listeners.clear();
        client.close();
        executorService.shutdownNow();
    }

    @Override
    public CuratorFramework nonNamespaceView()
    {
        Preconditions.checkState(state.get() == State.STARTED);

        return new NonNamespaceFacade(this);
    }

    @Override
    public CreateBuilder create()
    {
        Preconditions.checkState(state.get() == State.STARTED);

        return new CreateBuilderImpl(this);
    }

    @Override
    public DeleteBuilder delete()
    {
        Preconditions.checkState(state.get() == State.STARTED);

        return new DeleteBuilderImpl(this);
    }

    @Override
    public ExistsBuilder checkExists()
    {
        Preconditions.checkState(state.get() == State.STARTED);

        return new ExistsBuilderImpl(this);
    }

    @Override
    public GetDataBuilder getData()
    {
        Preconditions.checkState(state.get() == State.STARTED);

        return new GetDataBuilderImpl(this);
    }

    @Override
    public SetDataBuilder setData()
    {
        Preconditions.checkState(state.get() == State.STARTED);

        return new SetDataBuilderImpl(this);
    }

    @Override
    public GetChildrenBuilder getChildren()
    {
        Preconditions.checkState(state.get() == State.STARTED);

        return new GetChildrenBuilderImpl(this);
    }

    @Override
    public GetACLBuilder getACL()
    {
        Preconditions.checkState(state.get() == State.STARTED);

        return new GetACLBuilderImpl(this);
    }

    @Override
    public SetACLBuilder setACL()
    {
        Preconditions.checkState(state.get() == State.STARTED);

        return new SetACLBuilderImpl(this);
    }

    @Override
    public void     addListener(CuratorListener listener)
    {
        addListener(listener, MoreExecutors.sameThreadExecutor());
    }

    @Override
    public void     addListener(CuratorListener listener, Executor executor)
    {
        listeners.put(listener, new ListenerEntry<CuratorListener>(listener, executor));
    }

    @Override
    public void     removeListener(CuratorListener listener)
    {
        listeners.remove(listener);
    }

    @Override
    public void sync(String path, Object context)
    {
        Preconditions.checkState(state.get() == State.STARTED);

        path = fixForNamespace(path);

        internalSync(this, path, context);
    }

    protected void internalSync(CuratorFrameworkImpl impl, String path, Object context)
    {
        BackgroundOperation<String> operation = new BackgroundSyncImpl(impl, context);
        backgroundOperations.offer(new OperationAndData<String>(operation, path, null));
    }

    @Override
    public CuratorZookeeperClient getZookeeperClient()
    {
        return client;
    }

    @Override
    public EnsurePath newNamespaceAwareEnsurePath(String path)
    {
        return new EnsurePath(fixForNamespace(path));
    }

    RetryLoop newRetryLoop()
    {
        return client.newRetryLoop();
    }

    ZooKeeper getZooKeeper() throws Exception
    {
        return client.getZooKeeper();
    }

    <DATA_TYPE> void processBackgroundOperation(OperationAndData<DATA_TYPE> operationAndData, CuratorEvent event)
    {
        boolean     queueOperation = false;
        do
        {
            if ( event == null )
            {
                queueOperation = true;
                break;
            }

            if ( RetryLoop.shouldRetry(event.getResultCode()) )
            {
                if ( client.getRetryPolicy().allowRetry(operationAndData.getThenIncrementRetryCount(), operationAndData.getElapsedTimeMs()) )
                {
                    queueOperation = true;
                }
                else
                {
                    notifyErrorClosing(event.getResultCode(), null);
                }
                break;
            }

            if ( operationAndData.getCallback() != null )
            {
                sendToBackgroundCallback(operationAndData, event);
                break;
            }

            processEvent(event);
        } while ( false );

        if ( queueOperation )
        {
            backgroundOperations.offer(operationAndData);
        }
    }

    void   notifyErrorClosing(final int resultCode, final Throwable e)
    {
        if ( e != null )
        {
            client.getLog().error("Closing due to error", e);
        }
        else
        {
            client.getLog().error("Closing due to error code: " + resultCode);
        }

        client.close();
        for ( final ListenerEntry<CuratorListener> entry : listeners.values() )
        {
            entry.executor.execute
            (
                new Runnable()
                {
                    @Override
                    public void run()
                    {
                        entry.listener.clientClosedDueToError(CuratorFrameworkImpl.this, resultCode, e);
                    }
                }
            );
        }
    }

    String    unfixForNamespace(String path)
    {
        if ( namespace != null )
        {
            String      namespacePath = ZKPaths.makePath(namespace, null);
            if ( path.startsWith(namespacePath) )
            {
                path = (path.length() > namespacePath.length()) ? path.substring(namespacePath.length()) : "/";
            }
        }
        return path;
    }

    String    fixForNamespace(String path)
    {
        if ( !ensurePath() )
        {
            return "";
        }
        return ZKPaths.fixForNamespace(namespace, path);
    }

    private boolean ensurePath()
    {
        if ( ensurePath != null )
        {
            try
            {
                ensurePath.ensure(client);
            }
            catch ( Exception e )
            {
                notifyErrorClosing(0, e);
                return false;
            }
        }
        return true;
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

    private void handleBackgroundOperationException(OperationAndData<?> operationAndData, Throwable e)
    {
        do
        {
            if ( (operationAndData != null) && RetryLoop.isRetryException(e) )
            {
                client.getLog().debug("Retry-able exception received", e);
                if ( client.getRetryPolicy().allowRetry(operationAndData.getThenIncrementRetryCount(), operationAndData.getElapsedTimeMs()) )
                {
                    client.getLog().debug("Retrying operation");
                    backgroundOperations.offer(operationAndData);
                    break;
                }
                else
                {
                    client.getLog().debug("Retry policy did not allow retry");
                }
            }

            notifyErrorClosing(0, e);
        } while ( false );
    }

    private void backgroundOperationsLoop()
    {
        AuthInfo    auth = authInfo.getAndSet(null);
        if ( auth != null )
        {
            try
            {
                client.getZooKeeper().addAuthInfo(auth.scheme, auth.auth);
            }
            catch ( Exception e )
            {
                notifyErrorClosing(0, e);
                return;
            }
        }

        while ( !Thread.interrupted() )
        {
            OperationAndData<?>         operationAndData;
            try
            {
                operationAndData = backgroundOperations.take();
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                break;
            }

            try
            {
                operationAndData.callPerformBackgroundOperation();
            }
            catch ( Throwable e )
            {
                handleBackgroundOperationException(operationAndData, e);
            }
        }
    }

    private void processEvent(final CuratorEvent curatorEvent)
    {
        for ( final ListenerEntry<CuratorListener> entry : listeners.values() )
        {
            entry.executor.execute
            (
                new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            TimeTrace trace = client.startTracer("EventListener");
                            entry.listener.eventReceived(CuratorFrameworkImpl.this, curatorEvent);
                            trace.commit();
                        }
                        catch ( Exception e )
                        {
                            notifyErrorClosing(0, e);   // TODO - I'm not sure we should close here?
                        }
                    }
                }
            );
        }
    }
}
