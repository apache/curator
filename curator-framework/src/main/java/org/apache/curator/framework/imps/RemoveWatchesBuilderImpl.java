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

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import org.apache.curator.RetryLoop;
import org.apache.curator.TimeTrace;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.BackgroundPathable;
import org.apache.curator.framework.api.BackgroundPathableQuietly;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.api.Pathable;
import org.apache.curator.framework.api.RemoveWatchesLocal;
import org.apache.curator.framework.api.RemoveWatchesBuilder;
import org.apache.curator.framework.api.RemoveWatchesType;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.WatcherType;
import org.apache.zookeeper.ZooKeeper;


public class RemoveWatchesBuilderImpl implements RemoveWatchesBuilder, RemoveWatchesType, RemoveWatchesLocal, BackgroundOperation<String>
{
    private CuratorFrameworkImpl client;
    private Watcher watcher;
    private WatcherType watcherType;
    private boolean local;
    private boolean quietly;
    private Backgrounding backgrounding;
    
    public RemoveWatchesBuilderImpl(CuratorFrameworkImpl client)
    {
        this.client = client;
        this.watcher = null;
        this.watcherType = null;
        this.local = false;
        this.quietly = false;
        this.backgrounding = new Backgrounding();
    }

    void prepInternalRemoval(Watcher watcher)
    {
        this.watcher = watcher;
        watcherType = WatcherType.Any;
        quietly = true;
        this.backgrounding = new Backgrounding();
    }

    @Override
    public RemoveWatchesType remove(Watcher watcher)
    {
        this.watcher = watcher == null ? null : client.getNamespaceWatcherMap().getNamespaceWatcher(watcher);
        return this;
    }
    
    @Override
    public RemoveWatchesType remove(CuratorWatcher watcher)
    {
        this.watcher = watcher == null ? null : client.getNamespaceWatcherMap().getNamespaceWatcher(watcher);
        return this;
    }    

    @Override
    public RemoveWatchesType removeAll()
    {
        this.watcher = null;
        return this;
    }

    @Override
    public RemoveWatchesLocal ofType(WatcherType watcherType)
    {
        this.watcherType = watcherType;
        
        return this;
    }

    @Override
    public Pathable<Void> inBackground(BackgroundCallback callback, Object context)
    {
        backgrounding = new Backgrounding(callback, context);
        return this;
    }

    @Override
    public Pathable<Void> inBackground(BackgroundCallback callback, Object context, Executor executor)
    {
        backgrounding = new Backgrounding(client, callback, context, executor);
        return this;
    }

    @Override
    public Pathable<Void> inBackground(BackgroundCallback callback)
    {
        backgrounding = new Backgrounding(callback);
        return this;
    }

    @Override
    public Pathable<Void> inBackground(BackgroundCallback callback, Executor executor)
    {
        backgrounding = new Backgrounding(client, callback, executor);
        return this;
    }

    @Override
    public Pathable<Void> inBackground()
    {
        backgrounding = new Backgrounding(true);
        return this;
    }

    @Override
    public Pathable<Void> inBackground(Object context)
    {
        backgrounding = new Backgrounding(context);
        return this;
    }

    @Override
    public BackgroundPathableQuietly<Void> local()
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
    
    private void pathInBackground(String path)
    {
        OperationAndData.ErrorCallback<String>  errorCallback = null;        
        client.processBackgroundOperation(new OperationAndData<String>(this, path, backgrounding.getCallback(), errorCallback, backgrounding.getContext()), null);
    }
    
    void pathInForeground(final String path) throws Exception
    {
        RetryLoop.callWithRetry(client.getZookeeperClient(), 
                new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        try
                        {
                            ZooKeeper zkClient = client.getZooKeeper();
                            if(watcher == null)
                            {
                                zkClient.removeAllWatches(path, watcherType, local);    
                            }
                            else
                            {
                                zkClient.removeWatches(path, watcher, watcherType, local);
                            }
                        }
                        catch(KeeperException.NoWatcherException e)
                        {
                            //Swallow this exception if the quietly flag is set, otherwise rethrow.
                            if(!quietly)
                            {
                                throw e;
                            }
                        }
                     
                        return null;
                    }
                });
    }
    
    @Override
    public void performBackgroundOperation(final OperationAndData<String> operationAndData)
            throws Exception
    {
        final TimeTrace   trace = client.getZookeeperClient().startTracer("RemoteWatches-Background");
        
        AsyncCallback.VoidCallback callback = new AsyncCallback.VoidCallback()
        {
            @Override
            public void processResult(int rc, String path, Object ctx)
            {
                trace.commit();
                CuratorEvent event = new CuratorEventImpl(client, CuratorEventType.REMOVE_WATCHES, rc, path, null, ctx, null, null, null, null, null);
                client.processBackgroundOperation(operationAndData, event);                
            }
        };
        
        ZooKeeper zkClient = client.getZooKeeper();
        if(watcher == null)
        {
            zkClient.removeAllWatches(operationAndData.getData(), watcherType, local, callback, operationAndData.getContext());    
        }
        else
        {
            zkClient.removeWatches(operationAndData.getData(), watcher, watcherType, local, callback, operationAndData.getContext());
        }
        
    }
}