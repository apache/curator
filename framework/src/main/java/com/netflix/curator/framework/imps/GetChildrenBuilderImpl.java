/*
 Copyright 2011 Netflix, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 */
package com.netflix.curator.framework.imps;

import com.google.common.collect.Lists;
import com.netflix.curator.RetryLoop;
import com.netflix.curator.TimeTrace;
import com.netflix.curator.framework.BackgroundCallback;
import com.netflix.curator.framework.BackgroundPathable;
import com.netflix.curator.framework.CuratorEventType;
import com.netflix.curator.framework.GetChildrenBuilder;
import com.netflix.curator.framework.Pathable;
import com.netflix.curator.framework.WatchPathable;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import java.util.List;
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
        };
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
        watching = new Watching(watcher);
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
                CuratorEventImpl event = new CuratorEventImpl(CuratorEventType.CHILDREN, rc, path, null, o, stat, null, strings, null, null);
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
            client.processBackgroundOperation(new OperationAndData<String>(this, path, backgrounding.getCallback()), null);
        }
        else
        {
            children = pathInForeground(path);
        }
        return children;
    }

    private List<String> pathInForeground(String path) throws Exception
    {
        List<String>    children = null;

        TimeTrace trace = client.getZookeeperClient().startTracer("GetChildrenBuilderImpl-Foreground");
        RetryLoop retryLoop = client.newRetryLoop();
        while ( retryLoop.shouldContinue() )
        {
            try
            {
                if ( watching.isWatched() )
                {
                    children = client.getZooKeeper().getChildren(path, true, responseStat);
                }
                else
                {
                    children = client.getZooKeeper().getChildren(path, watching.getWatcher(), responseStat);
                }
                retryLoop.markComplete();
            }
            catch ( Exception e )
            {
                retryLoop.takeException(e);
            }
        }
        trace.commit();
        return children;
    }
}
