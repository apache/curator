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
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.BackgroundStatable;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.api.Ensembleable;
import org.apache.curator.framework.api.GetConfigBuilder;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

public class GetConfigBuilderImpl implements GetConfigBuilder, BackgroundOperation<Void>
{
    private final CuratorFrameworkImpl client;

    private Backgrounding backgrounding;
    private Watching watching;
    private Stat stat;

    public GetConfigBuilderImpl(CuratorFrameworkImpl client)
    {
        this.client = client;
        backgrounding = new Backgrounding();
        watching = new Watching();
    }

    @Override
    public Ensembleable<byte[]> storingStatIn(Stat stat)
    {
        this.stat = stat;
        return this;
    }

    @Override
    public BackgroundStatable<Ensembleable<byte[]>> watched()
    {
        watching = new Watching(true);
        return this;
    }

    @Override
    public GetConfigBuilder usingWatcher(Watcher watcher)
    {
        watching = new Watching(client, watcher);
        return this;
    }

    @Override
    public GetConfigBuilder usingWatcher(final CuratorWatcher watcher)
    {
        watching = new Watching(client, watcher);
        return this;
    }

    @Override
    public Ensembleable<byte[]> inBackground()
    {
        backgrounding = new Backgrounding(true);
        return this;
    }

    @Override
    public Ensembleable<byte[]> inBackground(Object context)
    {
        backgrounding = new Backgrounding(context);
        return this;
    }

    @Override
    public Ensembleable<byte[]> inBackground(BackgroundCallback callback)
    {
        backgrounding = new Backgrounding(callback);
        return this;
    }

    @Override
    public Ensembleable<byte[]> inBackground(BackgroundCallback callback, Object context)
    {
        backgrounding = new Backgrounding(callback, context);
        return this;
    }

    @Override
    public Ensembleable<byte[]> inBackground(BackgroundCallback callback, Executor executor)
    {
        backgrounding = new Backgrounding(callback, executor);
        return this;
    }

    @Override
    public Ensembleable<byte[]> inBackground(BackgroundCallback callback, Object context, Executor executor)
    {
        backgrounding = new Backgrounding(client, callback, context, executor);
        return this;
    }

    @Override
    public byte[] forEnsemble() throws Exception
    {
        if ( backgrounding.inBackground() )
        {
            client.processBackgroundOperation(new OperationAndData<Void>(this, null, backgrounding.getCallback(), null, backgrounding.getContext()), null);
            return null;
        }
        else
        {
            return configInForeground();
        }
    }

    @Override
    public void performBackgroundOperation(final OperationAndData<Void> operationAndData) throws Exception
    {
        final TimeTrace trace = client.getZookeeperClient().startTracer("GetDataBuilderImpl-Background");
        AsyncCallback.DataCallback callback = new AsyncCallback.DataCallback()
        {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat)
            {
                trace.commit();
                CuratorEvent event = new CuratorEventImpl(client, CuratorEventType.GET_CONFIG, rc, path, null, ctx, stat, data, null, null, null);
                client.processBackgroundOperation(operationAndData, event);
            }
        };
        if ( watching.isWatched() )
        {
            client.getZooKeeper().getConfig(true, callback, backgrounding.getContext());
        }
        else
        {
            client.getZooKeeper().getConfig(watching.getWatcher(), callback, backgrounding.getContext());
        }
    }

    private byte[] configInForeground() throws Exception
    {
        TimeTrace trace = client.getZookeeperClient().startTracer("GetConfigBuilderImpl-Foreground");
        try
        {
            return RetryLoop.callWithRetry
            (
                client.getZookeeperClient(),
                new Callable<byte[]>()
                {
                    @Override
                    public byte[] call() throws Exception
                    {
                        if ( watching.isWatched() )
                        {
                            return client.getZooKeeper().getConfig(true, stat);
                        }
                        return client.getZooKeeper().getConfig(watching.getWatcher(), stat);
                    }
                }
            );
        }
        finally
        {
            trace.commit();
        }
    }
}
