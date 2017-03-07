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

import org.apache.curator.RetryLoop;
import org.apache.curator.TimeTrace;
import org.apache.curator.framework.api.*;
import org.apache.curator.utils.DebugUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.WatcherType;
import org.apache.zookeeper.ZooKeeper;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;


public class RemoveWatchesBuilderImpl implements RemoveWatchesBuilder, RemoveWatchesType, RemoveWatchesLocal, BackgroundOperation<String>, ErrorListenerPathable<Void>
{
    private CuratorFrameworkImpl client;
    private Watcher watcher;
    private CuratorWatcher curatorWatcher;
    private WatcherType watcherType;
    private boolean guaranteed;
    private boolean local;
    private boolean quietly;    
    private Backgrounding backgrounding;
    
    public RemoveWatchesBuilderImpl(CuratorFrameworkImpl client)
    {
        this.client = client;
        this.watcher = null;
        this.curatorWatcher = null;
        this.watcherType = WatcherType.Any;
        this.guaranteed = false;
        this.local = false;
        this.quietly = false;
        this.backgrounding = new Backgrounding();
    }

    public RemoveWatchesBuilderImpl(CuratorFrameworkImpl client, Watcher watcher, CuratorWatcher curatorWatcher, WatcherType watcherType, boolean guaranteed, boolean local, boolean quietly, Backgrounding backgrounding)
    {
        this.client = client;
        this.watcher = watcher;
        this.curatorWatcher = curatorWatcher;
        this.watcherType = watcherType;
        this.guaranteed = guaranteed;
        this.local = local;
        this.quietly = quietly;
        this.backgrounding = backgrounding;
    }

    void internalRemoval(Watcher watcher, String path) throws Exception
    {
        this.watcher = watcher;
        watcherType = WatcherType.Any;
        quietly = true;
        guaranteed = true;
        if ( Boolean.getBoolean(DebugUtils.PROPERTY_REMOVE_WATCHERS_IN_FOREGROUND) )
        {
            this.backgrounding = new Backgrounding();
            pathInForeground(path);
        }
        else
        {
            this.backgrounding = new Backgrounding(true);
            pathInBackground(path);
        }
    }

    @Override
    public RemoveWatchesType remove(Watcher watcher)
    {
        this.watcher = watcher;
        this.curatorWatcher = null;
        return this;
    }
    
    @Override
    public RemoveWatchesType remove(CuratorWatcher watcher)
    {
        this.watcher = null;
        this.curatorWatcher = watcher;
        return this;
    }    

    @Override
    public RemoveWatchesType removeAll()
    {
        this.watcher = null;
        this.curatorWatcher = null;
        return this;
    }

    @Override
    public RemoveWatchesLocal ofType(WatcherType watcherType)
    {
        this.watcherType = watcherType;
        
        return this;
    }

    @Override
    public ErrorListenerPathable<Void> inBackground(BackgroundCallback callback, Object context)
    {
        backgrounding = new Backgrounding(callback, context);
        return this;
    }

    @Override
    public ErrorListenerPathable<Void> inBackground(BackgroundCallback callback, Object context, Executor executor)
    {
        backgrounding = new Backgrounding(client, callback, context, executor);
        return this;
    }

    @Override
    public ErrorListenerPathable<Void> inBackground(BackgroundCallback callback)
    {
        backgrounding = new Backgrounding(callback);
        return this;
    }

    @Override
    public ErrorListenerPathable<Void> inBackground(BackgroundCallback callback, Executor executor)
    {
        backgrounding = new Backgrounding(client, callback, executor);
        return this;
    }

    @Override
    public ErrorListenerPathable<Void> inBackground()
    {
        backgrounding = new Backgrounding(true);
        return this;
    }

    @Override
    public ErrorListenerPathable<Void> inBackground(Object context)
    {
        backgrounding = new Backgrounding(context);
        return this;
    }

    @Override
    public Pathable<Void> withUnhandledErrorListener(UnhandledErrorListener listener)
    {
        backgrounding = new Backgrounding(backgrounding, listener);
        return this;
    }

    @Override
    public RemoveWatchesLocal guaranteed()
    {
        guaranteed = true;
        return this;
    }    

    @Override
    public BackgroundPathableQuietlyable<Void> locally()
    {
        local = true;
        return this;
    }
    
    @Override
    public BackgroundPathable<Void> quietly()
    {
        quietly = true;
        return this;
    }
    
    @Override
    public Void forPath(String path) throws Exception
    {
        final String adjustedPath = client.fixForNamespace(path);
        
        if(backgrounding.inBackground())
        {
            pathInBackground(adjustedPath);
        }
        else
        {
            pathInForeground(adjustedPath);
        }        
        
        return null;
    }    
    
    private void pathInBackground(final String path)
    {
        OperationAndData.ErrorCallback<String>  errorCallback = null;
        
        //Only need an error callback if we're in guaranteed mode
        if(guaranteed)
        {
            errorCallback = new OperationAndData.ErrorCallback<String>()
            {
                @Override
                public void retriesExhausted(OperationAndData<String> operationAndData)
                {
                    client.getFailedRemoveWatcherManager().addFailedOperation(new FailedRemoveWatchManager.FailedRemoveWatchDetails(path, watcher));
                }            
            };
        }
        
        client.processBackgroundOperation(new OperationAndData<String>(this, path, backgrounding.getCallback(),
                                                                       errorCallback, backgrounding.getContext(), !local), null);
    }
    
    private void pathInForeground(final String path) throws Exception
    {
        NamespaceWatcher namespaceWatcher = makeNamespaceWatcher(path);
        //For the local case we don't want to use the normal retry loop and we don't want to block until a connection is available.
        //We just execute the removeWatch, and if it fails, ZK will just remove local watches.
        if ( local )
        {
            ZooKeeper zkClient = client.getZooKeeper();
            if ( namespaceWatcher != null )
            {
                zkClient.removeWatches(path, namespaceWatcher, watcherType, local);
            }
            else
            {
                zkClient.removeAllWatches(path, watcherType, local);
            }
        }
        else
        {
            final NamespaceWatcher finalNamespaceWatcher = namespaceWatcher;
            RetryLoop.callWithRetry(client.getZookeeperClient(),
                    new Callable<Void>()
                    {
                        @Override
                        public Void call() throws Exception
                        {
                            try
                            {
                                ZooKeeper zkClient = client.getZookeeperClient().getZooKeeper();

                                if ( finalNamespaceWatcher != null )
                                {
                                    zkClient.removeWatches(path, finalNamespaceWatcher, watcherType, false);
                                }
                                else
                                {
                                    zkClient.removeAllWatches(path, watcherType, false);
                                }
                            }
                            catch(Exception e)
                            {
                                if( RetryLoop.isRetryException(e) && guaranteed )
                                {
                                    //Setup the guaranteed handler
                                    client.getFailedRemoveWatcherManager().addFailedOperation(new FailedRemoveWatchManager.FailedRemoveWatchDetails(path, finalNamespaceWatcher));
                                    throw e;
                                }
                                else if(e instanceof KeeperException.NoWatcherException && quietly)
                                {
                                    // ignore
                                }
                                else
                                {
                                    //Rethrow
                                    throw e;
                                }
                            }
                     
                            return null;
                        }
            });
        }
    }

    private NamespaceWatcher makeNamespaceWatcher(String path)
    {
        NamespaceWatcher namespaceWatcher = null;
        if ( watcher != null )
        {
            if ( watcher instanceof NamespaceWatcher )
            {
                namespaceWatcher = (NamespaceWatcher)watcher;
            }
            else
            {
                namespaceWatcher = new NamespaceWatcher(client, watcher, path);
            }
        }
        else if ( curatorWatcher != null )
        {
            namespaceWatcher = new NamespaceWatcher(client, curatorWatcher, path);
        }
        return namespaceWatcher;
    }

    @Override
    public void performBackgroundOperation(final OperationAndData<String> operationAndData)
            throws Exception
    {
        try
        {
            final TimeTrace   trace = client.getZookeeperClient().startTracer("RemoteWatches-Background");

            AsyncCallback.VoidCallback callback = new AsyncCallback.VoidCallback()
            {
                @Override
                public void processResult(int rc, String path, Object ctx)
                {
                    trace.commit();
                    CuratorEvent event = new CuratorEventImpl(client, CuratorEventType.REMOVE_WATCHES, rc, path, null, ctx, null, null, null, null, null, null);
                    client.processBackgroundOperation(operationAndData, event);
                }
            };

            ZooKeeper zkClient = client.getZooKeeper();
            NamespaceWatcher namespaceWatcher = makeNamespaceWatcher(operationAndData.getData());
            if(namespaceWatcher == null)
            {
                zkClient.removeAllWatches(operationAndData.getData(), watcherType, local, callback, operationAndData.getContext());
            }
            else
            {
                zkClient.removeWatches(operationAndData.getData(), namespaceWatcher, watcherType, local, callback, operationAndData.getContext());
            }
        }
        catch ( Throwable e )
        {
            backgrounding.checkError(e, null);
        }
    }
}