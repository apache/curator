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
import org.apache.curator.drivers.OperationTrace;
import org.apache.curator.framework.api.AddPersistentWatchBuilder;
import org.apache.curator.framework.api.AddPersistentWatchBuilder2;
import org.apache.curator.framework.api.AddPersistentWatchable;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.api.Pathable;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.Watcher;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

public class AddPersistentWatchBuilderImpl implements AddPersistentWatchBuilder, Pathable<Void>, BackgroundOperation<String>
{
    private final CuratorFrameworkImpl client;
    private Watching watching = null;
    private Backgrounding backgrounding = new Backgrounding();
    private boolean recursive = false;

    AddPersistentWatchBuilderImpl(CuratorFrameworkImpl client)
    {
        this.client = client;
    }

    public AddPersistentWatchBuilderImpl(CuratorFrameworkImpl client, Watching watching, Backgrounding backgrounding, boolean recursive)
    {
        this.client = client;
        this.watching = watching;
        this.backgrounding = backgrounding;
        this.recursive = recursive;
    }

    @Override
    public AddPersistentWatchable<Pathable<Void>> inBackground()
    {
        backgrounding = new Backgrounding();
        return this;
    }

    @Override
    public AddPersistentWatchBuilder2 recursive()
    {
        recursive = true;
        return this;
    }

    @Override
    public Pathable<Void> usingWatcher(Watcher watcher)
    {
        watching = new Watching(client, watcher);
        return this;
    }

    @Override
    public Pathable<Void> usingWatcher(CuratorWatcher watcher)
    {
        watching = new Watching(client, watcher);
        return this;
    }

    @Override
    public AddPersistentWatchable<Pathable<Void>> inBackground(Object context)
    {
        backgrounding = new Backgrounding(context);
        return this;
    }

    @Override
    public AddPersistentWatchable<Pathable<Void>> inBackground(BackgroundCallback callback)
    {
        backgrounding = new Backgrounding(callback);
        return this;
    }

    @Override
    public AddPersistentWatchable<Pathable<Void>> inBackground(BackgroundCallback callback, Object context)
    {
        backgrounding = new Backgrounding(callback, context);
        return this;
    }

    @Override
    public AddPersistentWatchable<Pathable<Void>> inBackground(BackgroundCallback callback, Executor executor)
    {
        backgrounding = new Backgrounding(callback, executor);
        return this;
    }

    @Override
    public AddPersistentWatchable<Pathable<Void>> inBackground(BackgroundCallback callback, Object context, Executor executor)
    {
        backgrounding = new Backgrounding(client, callback, context, executor);
        return this;
    }

    @Override
    public Void forPath(String path) throws Exception
    {
        if ( backgrounding.inBackground() )
        {
            client.processBackgroundOperation(new OperationAndData<>(this, path, backgrounding.getCallback(), null, backgrounding.getContext(), watching), null);
        }
        else
        {
            pathInForeground(path);
        }
        return null;
    }

    @Override
    public void performBackgroundOperation(final OperationAndData<String> data) throws Exception
    {
        String path = data.getData();
        String fixedPath = client.fixForNamespace(path);
        try
        {
            final OperationTrace   trace = client.getZookeeperClient().startAdvancedTracer("AddPersistentWatchBuilderImpl-Background");
            client.getZooKeeper().addPersistentWatch
                (
                    fixedPath,
                    watching.getWatcher(path),
                    recursive,
                    new AsyncCallback.VoidCallback()
                    {
                        @Override
                        public void processResult(int rc, String path, Object ctx)
                        {
                            trace.setReturnCode(rc).setWithWatcher(true).setPath(path).commit();
                            CuratorEvent event = new CuratorEventImpl(client, CuratorEventType.ADD_PERSISTENT_WATCH, rc, path, null, ctx, null, null, null, null, null, null);
                            client.processBackgroundOperation(data, event);
                        }
                    },
                    backgrounding.getContext()
                );
        }
        catch ( Throwable e )
        {
            backgrounding.checkError(e, watching);
        }
    }

    private void pathInForeground(final String path) throws Exception
    {
        final String fixedPath = client.fixForNamespace(path);
        OperationTrace trace = client.getZookeeperClient().startAdvancedTracer("AddPersistentWatchBuilderImpl-Foreground");
        RetryLoop.callWithRetry
        (
            client.getZookeeperClient(),
            new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    client.getZooKeeper().addPersistentWatch(fixedPath, watching.getWatcher(path), recursive);
                    return null;
                }
            }
        );
        trace.setPath(fixedPath).setWithWatcher(true).commit();
    }
}
