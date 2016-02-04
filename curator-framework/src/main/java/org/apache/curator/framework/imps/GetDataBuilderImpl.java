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
import org.apache.curator.framework.api.BackgroundPathable;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.framework.api.GetDataWatchBackgroundStatable;
import org.apache.curator.framework.api.Pathable;
import org.apache.curator.framework.api.WatchPathable;
import org.apache.curator.utils.ThreadUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

class GetDataBuilderImpl implements GetDataBuilder, BackgroundOperation<String>
{
    private final Logger                log = LoggerFactory.getLogger(getClass());
    private final CuratorFrameworkImpl  client;
    private Stat                        responseStat;
    private Watching                    watching;
    private Backgrounding               backgrounding;
    private boolean                     decompress;

    GetDataBuilderImpl(CuratorFrameworkImpl client)
    {
        this.client = client;
        responseStat = null;
        watching = new Watching();
        backgrounding = new Backgrounding();
        decompress = false;
    }

    @Override
    public GetDataWatchBackgroundStatable decompressed()
    {
        decompress = true;
        return new GetDataWatchBackgroundStatable()
        {
            @Override
            public Pathable<byte[]> inBackground()
            {
                return GetDataBuilderImpl.this.inBackground();
            }

            @Override
            public Pathable<byte[]> inBackground(BackgroundCallback callback, Object context)
            {
                return GetDataBuilderImpl.this.inBackground(callback, context);
            }

            @Override
            public Pathable<byte[]> inBackground(BackgroundCallback callback, Object context, Executor executor)
            {
                return GetDataBuilderImpl.this.inBackground(callback, context, executor);
            }

            @Override
            public Pathable<byte[]> inBackground(Object context)
            {
                return GetDataBuilderImpl.this.inBackground(context);
            }

            @Override
            public Pathable<byte[]> inBackground(BackgroundCallback callback)
            {
                return GetDataBuilderImpl.this.inBackground(callback);
            }

            @Override
            public Pathable<byte[]> inBackground(BackgroundCallback callback, Executor executor)
            {
                return GetDataBuilderImpl.this.inBackground(callback, executor);
            }

            @Override
            public byte[] forPath(String path) throws Exception
            {
                return GetDataBuilderImpl.this.forPath(path);
            }

            @Override
            public WatchPathable<byte[]> storingStatIn(Stat stat)
            {
                return GetDataBuilderImpl.this.storingStatIn(stat);
            }

            @Override
            public BackgroundPathable<byte[]> watched()
            {
                return GetDataBuilderImpl.this.watched();
            }

            @Override
            public BackgroundPathable<byte[]> usingWatcher(Watcher watcher)
            {
                return GetDataBuilderImpl.this.usingWatcher(watcher);
            }

            @Override
            public BackgroundPathable<byte[]> usingWatcher(CuratorWatcher watcher)
            {
                return GetDataBuilderImpl.this.usingWatcher(watcher);
            }
        };
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

            @Override
            public Pathable<byte[]> usingWatcher(CuratorWatcher watcher)
            {
                GetDataBuilderImpl.this.usingWatcher(watcher);
                return GetDataBuilderImpl.this;
            }
        };
    }

    @Override
    public Pathable<byte[]> inBackground(BackgroundCallback callback, Object context)
    {
        backgrounding = new Backgrounding(callback, context);
        return this;
    }

    @Override
    public Pathable<byte[]> inBackground(BackgroundCallback callback, Object context, Executor executor)
    {
        backgrounding = new Backgrounding(client, callback, context, executor);
        return this;
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
    public BackgroundPathable<byte[]> usingWatcher(CuratorWatcher watcher)
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
                if ( decompress && (data != null) )
                {
                    try
                    {
                        data = client.getCompressionProvider().decompress(path, data);
                    }
                    catch ( Exception e )
                    {
                        ThreadUtils.checkInterrupted(e);
                        log.error("Decompressing for path: " + path, e);
                        rc = KeeperException.Code.DATAINCONSISTENCY.intValue();
                    }
                }
                CuratorEvent event = new CuratorEventImpl(client, CuratorEventType.GET_DATA, rc, path, null, ctx, stat, data, null, null, null, null);
                client.processBackgroundOperation(operationAndData, event);
            }
        };
        if ( watching.isWatched() )
        {
            client.getZooKeeper().getData(operationAndData.getData(), true, callback, backgrounding.getContext());
        }
        else
        {
            client.getZooKeeper().getData(operationAndData.getData(), watching.getWatcher(client, operationAndData.getData()), callback, backgrounding.getContext());
        }
    }

    @Override
    public byte[] forPath(String path) throws Exception
    {
        path = client.fixForNamespace(path);

        byte[]      responseData = null;
        if ( backgrounding.inBackground() )
        {
            client.processBackgroundOperation(new OperationAndData<String>(this, path, backgrounding.getCallback(), null, backgrounding.getContext()), null);
        }
        else
        {
            responseData = pathInForeground(path);
        }
        return responseData;
    }

    private byte[] pathInForeground(final String path) throws Exception
    {
        TimeTrace   trace = client.getZookeeperClient().startTracer("GetDataBuilderImpl-Foreground");
        byte[]      responseData = RetryLoop.callWithRetry
        (
            client.getZookeeperClient(),
            new Callable<byte[]>()
            {
                @Override
                public byte[] call() throws Exception
                {
                    byte[]      responseData;
                    if ( watching.isWatched() )
                    {
                        responseData = client.getZooKeeper().getData(path, true, responseStat);
                    }
                    else
                    {
                        responseData = client.getZooKeeper().getData(path, watching.getWatcher(client, path), responseStat);
                    }
                    return responseData;
                }
            }
        );
        trace.commit();

        return decompress ? client.getCompressionProvider().decompress(path, responseData) : responseData;
    }
}
