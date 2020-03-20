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
import org.apache.curator.framework.api.AddWatchBuilder;
import org.apache.curator.framework.api.AddWatchBuilder2;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.api.Pathable;
import org.apache.curator.framework.api.WatchableBase;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.Watcher;
import java.util.concurrent.Executor;

public class AddWatchBuilderImpl implements AddWatchBuilder, Pathable<Void>, BackgroundOperation<String>
{
    private final CuratorFrameworkImpl client;
    private Watching watching;
    private Backgrounding backgrounding = new Backgrounding();
    private AddWatchMode mode = AddWatchMode.PERSISTENT_RECURSIVE;

    AddWatchBuilderImpl(CuratorFrameworkImpl client)
    {
        this.client = client;
        watching = new Watching(client, true);
    }

    public AddWatchBuilderImpl(CuratorFrameworkImpl client, Watching watching, Backgrounding backgrounding, AddWatchMode mode)
    {
        this.client = client;
        this.watching = watching;
        this.backgrounding = backgrounding;
        this.mode = mode;
    }

    @Override
    public WatchableBase<Pathable<Void>> inBackground()
    {
        backgrounding = new Backgrounding();
        return this;
    }

    @Override
    public AddWatchBuilder2 withMode(AddWatchMode mode)
    {
        this.mode = mode;
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
    public WatchableBase<Pathable<Void>> inBackground(Object context)
    {
        backgrounding = new Backgrounding(context);
        return this;
    }

    @Override
    public WatchableBase<Pathable<Void>> inBackground(BackgroundCallback callback)
    {
        backgrounding = new Backgrounding(callback);
        return this;
    }

    @Override
    public WatchableBase<Pathable<Void>> inBackground(BackgroundCallback callback, Object context)
    {
        backgrounding = new Backgrounding(callback, context);
        return this;
    }

    @Override
    public WatchableBase<Pathable<Void>> inBackground(BackgroundCallback callback, Executor executor)
    {
        backgrounding = new Backgrounding(callback, executor);
        return this;
    }

    @Override
    public WatchableBase<Pathable<Void>> inBackground(BackgroundCallback callback, Object context, Executor executor)
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
            final OperationTrace   trace = client.getZookeeperClient().startAdvancedTracer("AddWatchBuilderImpl-Background");
            if ( watching.isWatched() )
            {
                client.getZooKeeper().addWatch
                    (
                        fixedPath,
                        mode,
                        (rc, path1, ctx) -> {
                            trace.setReturnCode(rc).setWithWatcher(true).setPath(path1).commit();
                            CuratorEvent event = new CuratorEventImpl(client, CuratorEventType.ADD_WATCH, rc, path1, null, ctx, null, null, null, null, null, null);
                            client.processBackgroundOperation(data, event);
                        },
                        backgrounding.getContext()
                    );
            }
            else
            {
                client.getZooKeeper().addWatch
                    (
                        fixedPath,
                        watching.getWatcher(path),
                        mode,
                        (rc, path1, ctx) -> {
                            trace.setReturnCode(rc).setWithWatcher(true).setPath(path1).commit();
                            CuratorEvent event = new CuratorEventImpl(client, CuratorEventType.ADD_WATCH, rc, path1, null, ctx, null, null, null, null, null, null);
                            client.processBackgroundOperation(data, event);
                        },
                        backgrounding.getContext()
                    );
            }
        }
        catch ( Throwable e )
        {
            backgrounding.checkError(e, watching);
        }
    }

    private void pathInForeground(final String path) throws Exception
    {
        final String fixedPath = client.fixForNamespace(path);
        OperationTrace trace = client.getZookeeperClient().startAdvancedTracer("AddWatchBuilderImpl-Foreground");
        RetryLoop.callWithRetry
        (
            client.getZookeeperClient(), () -> {
                if ( watching.isWatched() )
                {
                    client.getZooKeeper().addWatch(fixedPath, mode);
                }
                else
                {
                    client.getZooKeeper().addWatch(fixedPath, watching.getWatcher(path), mode);
                }
                return null;
            });
        trace.setPath(fixedPath).setWithWatcher(true).commit();
    }
}