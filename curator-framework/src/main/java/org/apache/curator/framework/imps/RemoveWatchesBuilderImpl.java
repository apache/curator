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
    private boolean guaranteed;
    private boolean local;
    private boolean quietly;    
    private Backgrounding backgrounding;
    
    public RemoveWatchesBuilderImpl(CuratorFrameworkImpl client)
    {
        this.client = client;
        this.watcher = null;
        this.watcherType = WatcherType.Any;
        this.guaranteed = false;
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
        if(watcher == null) {
            this.watcher = null;
        } else {
            //Try and get the namespaced version of the watcher.
            this.watcher = client.getNamespaceWatcherMap().get(watcher);
            
            //If this is not present then default to the original watcher. This shouldn't happen in practice unless the user
            //has added a watch directly to the ZK client rather than via the CuratorFramework.
            if(this.watcher == null) {
                this.watcher = watcher;
            }
        }

        return this;
    }
    
    @Override
    public RemoveWatchesType remove(CuratorWatcher watcher)
    {
        this.watcher = watcher == null ? null : client.getNamespaceWatcherMap().get(watcher);
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
    public RemoveWatchesLocal guaranteed()
    {
        guaranteed = true;
        return this;
    }    

    @Override
    public BackgroundPathableQuietly<Void> locally()
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
    
    void pathInForeground(final String path) throws Exception
    {
        //For the local case we don't want to use the normal retry loop and we don't want to block until a connection is available.
        //We just execute the removeWatch, and if it fails, ZK will just remove local watches.
        if(local)
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
        else
        {
            RetryLoop.callWithRetry(client.getZookeeperClient(), 
                    new Callable<Void>()
                    {
                        @Override
                        public Void call() throws Exception
                        {
                            try
                            {
                                ZooKeeper zkClient = client.getZookeeperClient().getZooKeeper();    
                                
                                if(watcher == null)
                                {
                                    zkClient.removeAllWatches(path, watcherType, local);    
                                }
                                else
                                {
                                    zkClient.removeWatches(path, watcher, watcherType, local);
                                }
                            }
                            catch(Exception e)
                            {
                                if( RetryLoop.isRetryException(e) && guaranteed )
                                {
                                    //Setup the guaranteed handler
                                    client.getFailedRemoveWatcherManager().addFailedOperation(new FailedRemoveWatchManager.FailedRemoveWatchDetails(path, watcher));
                                    throw e;
                                }
                                else if(e instanceof KeeperException.NoWatcherException && quietly)
                                {
                                    //Ignore
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