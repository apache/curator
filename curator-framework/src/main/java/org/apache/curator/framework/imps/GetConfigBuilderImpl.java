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
import org.apache.curator.framework.api.BackgroundEnsembleable;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.api.Ensembleable;
import org.apache.curator.framework.api.GetConfigBuilder;
import org.apache.curator.framework.api.WatchBackgroundEnsembleable;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
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
    public WatchBackgroundEnsembleable<byte[]> storingStatIn(Stat stat)
    {
        this.stat = stat;
        return new WatchBackgroundEnsembleable<byte[]>()
        {
            @Override
            public Ensembleable<byte[]> inBackground()
            {
                return GetConfigBuilderImpl.this.inBackground();
            }

            @Override
            public Ensembleable<byte[]> inBackground(Object context)
            {
                return GetConfigBuilderImpl.this.inBackground(context);
            }

            @Override
            public Ensembleable<byte[]> inBackground(BackgroundCallback callback)
            {
                return GetConfigBuilderImpl.this.inBackground(callback);
            }

            @Override
            public Ensembleable<byte[]> inBackground(BackgroundCallback callback, Object context)
            {
                return GetConfigBuilderImpl.this.inBackground(callback, context);
            }

            @Override
            public Ensembleable<byte[]> inBackground(BackgroundCallback callback, Executor executor)
            {
                return GetConfigBuilderImpl.this.inBackground(callback, executor);
            }

            @Override
            public Ensembleable<byte[]> inBackground(BackgroundCallback callback, Object context, Executor executor)
            {
                return GetConfigBuilderImpl.this.inBackground(callback, context, executor);
            }

            @Override
            public byte[] forEnsemble() throws Exception
            {
                return GetConfigBuilderImpl.this.forEnsemble();
            }

            @Override
            public BackgroundEnsembleable<byte[]> watched()
            {
                return GetConfigBuilderImpl.this.watched();
            }

            @Override
            public BackgroundEnsembleable<byte[]> usingWatcher(Watcher watcher)
            {
                return GetConfigBuilderImpl.this.usingWatcher(watcher);
            }

            @Override
            public BackgroundEnsembleable<byte[]> usingWatcher(CuratorWatcher watcher)
            {
                return GetConfigBuilderImpl.this.usingWatcher(watcher);
            }
        };
    }

    @Override
    public BackgroundEnsembleable<byte[]> watched()
    {
        watching = new Watching(true);
        return new InternalBackgroundEnsembleable();
    }

    @Override
    public BackgroundEnsembleable<byte[]> usingWatcher(Watcher watcher)
    {
        watching = new Watching(watcher);
        return new InternalBackgroundEnsembleable();
    }

    @Override
    public BackgroundEnsembleable<byte[]> usingWatcher(CuratorWatcher watcher)
    {
        watching = new Watching(watcher);
        return new InternalBackgroundEnsembleable();
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
                CuratorEvent event = new CuratorEventImpl(client, CuratorEventType.GET_CONFIG, rc, path, null, ctx, stat, data, null, null, null, null);
                client.processBackgroundOperation(operationAndData, event);
            }
        };
        if ( watching.isWatched() )
        {
            client.getZooKeeper().getConfig(true, callback, backgrounding.getContext());
        }
        else
        {
            client.getZooKeeper().getConfig(watching.getWatcher(client, ZooDefs.CONFIG_NODE), callback, backgrounding.getContext());
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
                        return client.getZooKeeper().getConfig(watching.getWatcher(client, ZooDefs.CONFIG_NODE), stat);
                    }
                }
            );
        }
        finally
        {
            trace.commit();
        }
    }

    private class InternalBackgroundEnsembleable implements BackgroundEnsembleable<byte[]>
    {
        @Override
        public Ensembleable<byte[]> inBackground()
        {
            return GetConfigBuilderImpl.this.inBackground();
        }

        @Override
        public Ensembleable<byte[]> inBackground(Object context)
        {
            return GetConfigBuilderImpl.this.inBackground(context);
        }

        @Override
        public Ensembleable<byte[]> inBackground(BackgroundCallback callback)
        {
            return GetConfigBuilderImpl.this.inBackground(callback);
        }

        @Override
        public Ensembleable<byte[]> inBackground(BackgroundCallback callback, Object context)
        {
            return GetConfigBuilderImpl.this.inBackground(callback, context);
        }

        @Override
        public Ensembleable<byte[]> inBackground(BackgroundCallback callback, Executor executor)
        {
            return GetConfigBuilderImpl.this.inBackground(callback, executor);
        }

        @Override
        public Ensembleable<byte[]> inBackground(BackgroundCallback callback, Object context, Executor executor)
        {
            return GetConfigBuilderImpl.this.inBackground(callback, context, executor);
        }

        @Override
        public byte[] forEnsemble() throws Exception
        {
            return GetConfigBuilderImpl.this.forEnsemble();
        }
    }
}
