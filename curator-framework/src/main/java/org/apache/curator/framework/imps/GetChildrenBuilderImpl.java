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

import com.google.common.collect.Lists;
import org.apache.curator.RetryLoop;
import org.apache.curator.TimeTrace;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.BackgroundPathable;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.api.Pathable;
import org.apache.curator.framework.api.WatchPathable;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

class GetChildrenBuilderImpl implements GetChildrenBuilder, BackgroundOperation<String>
{
    private final CuratorFrameworkImpl client;
    private Watching watching;
    private Backgrounding backgrounding;
    private Stat                                    responseStat;

    GetChildrenBuilderImpl(CuratorFrameworkImpl client)
    {
        this.client = client;
        watching = new Watching();
        backgrounding = new Backgrounding();
        responseStat = null;
    }

    @Override
    public WatchPathable<List<String>> storingStatIn(Stat stat)
    {
        responseStat = stat;
        return new WatchPathable<List<String>>()
        {
            @Override
            public List<String> forPath(String path) throws Exception
            {
                return GetChildrenBuilderImpl.this.forPath(path);
            }

            @Override
            public Pathable<List<String>> watched()
            {
                GetChildrenBuilderImpl.this.watched();
                return GetChildrenBuilderImpl.this;
            }

            @Override
            public Pathable<List<String>> usingWatcher(Watcher watcher)
            {
                GetChildrenBuilderImpl.this.usingWatcher(watcher);
                return GetChildrenBuilderImpl.this;
            }

            @Override
            public Pathable<List<String>> usingWatcher(CuratorWatcher watcher)
            {
                GetChildrenBuilderImpl.this.usingWatcher(watcher);
                return GetChildrenBuilderImpl.this;
            }
        };
    }

    @Override
    public Pathable<List<String>> inBackground(BackgroundCallback callback, Object context)
    {
        backgrounding = new Backgrounding(callback, context);
        return this;
    }

    @Override
    public Pathable<List<String>> inBackground(BackgroundCallback callback, Object context, Executor executor)
    {
        backgrounding = new Backgrounding(client, callback, context, executor);
        return this;
    }

    @Override
    public Pathable<List<String>> inBackground(BackgroundCallback callback)
    {
        backgrounding = new Backgrounding(callback);
        return this;
    }

    @Override
    public Pathable<List<String>> inBackground(BackgroundCallback callback, Executor executor)
    {
        backgrounding = new Backgrounding(client, callback, executor);
        return this;
    }

    @Override
    public Pathable<List<String>> inBackground()
    {
        backgrounding = new Backgrounding(true);
        return this;
    }

    @Override
    public Pathable<List<String>> inBackground(Object context)
    {
        backgrounding = new Backgrounding(context);
        return this;
    }

    @Override
    public BackgroundPathable<List<String>> watched()
    {
        watching = new Watching(true);
        return this;
    }

    @Override
    public BackgroundPathable<List<String>> usingWatcher(Watcher watcher)
    {
        watching = new Watching(client, watcher);
        return this;
    }

    @Override
    public BackgroundPathable<List<String>> usingWatcher(CuratorWatcher watcher)
    {
        watching = new Watching(client, watcher);
        return this;
    }

    @Override
    public void performBackgroundOperation(final OperationAndData<String> operationAndData) throws Exception
    {
        final TimeTrace       trace = client.getZookeeperClient().startTracer("GetChildrenBuilderImpl-Background");
        AsyncCallback.Children2Callback callback = new AsyncCallback.Children2Callback()
        {
            @Override
            public void processResult(int rc, String path, Object o, List<String> strings, Stat stat)
            {
                trace.commit();
                if ( strings == null )
                {
                    strings = Lists.newArrayList();
                }
                CuratorEventImpl event = new CuratorEventImpl(client, CuratorEventType.CHILDREN, rc, path, null, o, stat, null, strings, null, null);
                client.processBackgroundOperation(operationAndData, event);
            }
        };
        if ( watching.isWatched() )
        {
            client.getZooKeeper().getChildren(operationAndData.getData(), true, callback, backgrounding.getContext());
        }
        else
        {
            client.getZooKeeper().getChildren(operationAndData.getData(), watching.getWatcher(), callback, backgrounding.getContext());
        }
    }

    @Override
    public List<String> forPath(String path) throws Exception
    {
        path = client.fixForNamespace(path);

        List<String>        children = null;
        if ( backgrounding.inBackground() )
        {
            client.processBackgroundOperation(new OperationAndData<String>(this, path, backgrounding.getCallback(), null, backgrounding.getContext()), null);
        }
        else
        {
            children = pathInForeground(path);
        }
        return children;
    }

    private List<String> pathInForeground(final String path) throws Exception
    {
        TimeTrace       trace = client.getZookeeperClient().startTracer("GetChildrenBuilderImpl-Foreground");
        List<String>    children = RetryLoop.callWithRetry
        (
            client.getZookeeperClient(),
            new Callable<List<String>>()
            {
                @Override
                public List<String> call() throws Exception
                {
                    List<String>    children;
                    if ( watching.isWatched() )
                    {
                        children = client.getZooKeeper().getChildren(path, true, responseStat);
                    }
                    else
                    {
                        children = client.getZooKeeper().getChildren(path, watching.getWatcher(), responseStat);
                    }
                    return children;
                }
            }
        );
        trace.commit();
        return children;
    }
}
