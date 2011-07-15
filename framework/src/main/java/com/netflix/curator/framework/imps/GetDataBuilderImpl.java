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

import com.netflix.curator.RetryLoop;
import com.netflix.curator.TimeTrace;
import com.netflix.curator.framework.api.*;
import com.netflix.curator.framework.api.BackgroundCallback;
import com.netflix.curator.framework.api.CuratorEventType;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.GetDataBuilder;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import java.util.concurrent.Executor;

class GetDataBuilderImpl implements GetDataBuilder, BackgroundOperation<String>
{
    private final CuratorFrameworkImpl client;
    private Stat                                      responseStat;
    private Watching watching;
    private Backgrounding backgrounding;

    GetDataBuilderImpl(CuratorFrameworkImpl client)
    {
        this.client = client;
        responseStat = null;
        watching = new Watching();
        backgrounding = new Backgrounding();
    }

    @Override
    public WatchPathable<byte[]> storingStatIn(Stat stat)
    {
        this.responseStat = stat;
        return new WatchPathable<byte[]>()
        {
            @Override
            public byte[] forPath(String path) throws Exception
            {
                return GetDataBuilderImpl.this.forPath(path);
            }

            @Override
            public Pathable<byte[]> watched()
            {
                GetDataBuilderImpl.this.watched();
                return GetDataBuilderImpl.this;
            }

            @Override
            public Pathable<byte[]> usingWatcher(Watcher watcher)
            {
                GetDataBuilderImpl.this.usingWatcher(watcher);
                return GetDataBuilderImpl.this;
            }
        };
    }

    @Override
    public Pathable<byte[]> inBackground(BackgroundCallback callback)
    {
        backgrounding = new Backgrounding(callback);
        return this;
    }

    @Override
    public Pathable<byte[]> inBackground(BackgroundCallback callback, Executor executor)
    {
        backgrounding = new Backgrounding(client, callback, executor);
        return this;
    }

    @Override
    public Pathable<byte[]> inBackground()
    {
        backgrounding = new Backgrounding(true);
        return this;
    }

    @Override
    public Pathable<byte[]> inBackground(Object context)
    {
        backgrounding = new Backgrounding(context);
        return this;
    }

    @Override
    public BackgroundPathable<byte[]> watched()
    {
        watching = new Watching(true);
        return this;
    }

    @Override
    public BackgroundPathable<byte[]> usingWatcher(Watcher watcher)
    {
        watching = new Watching(watcher);
        return this;
    }

    @Override
    public void performBackgroundOperation(final OperationAndData<String> operationAndData) throws Exception
    {
        final TimeTrace   trace = client.getZookeeperClient().startTracer("GetDataBuilderImpl-Background");
        AsyncCallback.DataCallback callback = new AsyncCallback.DataCallback()
        {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat)
            {
                trace.commit();
                CuratorEvent event = new CuratorEventImpl(CuratorEventType.GET_DATA, rc, path, null, ctx, stat, data, null, null, null);
                client.processBackgroundOperation(operationAndData, event);
            }
        };
        if ( watching.isWatched() )
        {
            client.getZooKeeper().getData(operationAndData.getData(), true, callback, backgrounding.getContext());
        }
        else
        {
            client.getZooKeeper().getData(operationAndData.getData(), watching.getWatcher(), callback, backgrounding.getContext());
        }
    }

    @Override
    public byte[] forPath(String path) throws Exception
    {
        path = client.fixForNamespace(path);

        byte[]      responseData = null;
        if ( backgrounding.inBackground() )
        {
            client.processBackgroundOperation(new OperationAndData<String>(this, path, backgrounding.getCallback()), null);
        }
        else
        {
            responseData = pathInForeground(path, responseData);
        }
        return responseData;
    }

    private byte[] pathInForeground(String path, byte[] responseData) throws Exception
    {
        TimeTrace trace = client.getZookeeperClient().startTracer("GetDataBuilderImpl-Foreground");
        RetryLoop retryLoop = client.newRetryLoop();
        while ( retryLoop.shouldContinue() )
        {
            try
            {
                if ( watching.isWatched() )
                {
                    responseData = client.getZooKeeper().getData(path, true, responseStat);
                }
                else
                {
                    responseData = client.getZooKeeper().getData(path, watching.getWatcher(), responseStat);
                }
                retryLoop.markComplete();
            }
            catch ( Exception e )
            {
                retryLoop.takeException(e);
            }
        }
        trace.commit();
        return responseData;
    }
}
