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
import org.apache.curator.framework.api.*;
import org.apache.curator.framework.api.transaction.CuratorTransactionBridge;
import org.apache.curator.framework.api.transaction.OperationType;
import org.apache.curator.framework.api.transaction.TransactionSetDataBuilder;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

class SetDataBuilderImpl implements SetDataBuilder, BackgroundOperation<PathAndBytes>, ErrorListenerPathAndBytesable<Stat>
{
    private final CuratorFrameworkImpl      client;
    private Backgrounding                   backgrounding;
    private int                             version;
    private boolean                         compress;

    SetDataBuilderImpl(CuratorFrameworkImpl client)
    {
        this.client = client;
        backgrounding = new Backgrounding();
        version = -1;
        compress = false;
    }

    TransactionSetDataBuilder   asTransactionSetDataBuilder(final CuratorTransactionImpl curatorTransaction, final CuratorMultiTransactionRecord transaction)
    {
        return new TransactionSetDataBuilder()
        {
            @Override
            public CuratorTransactionBridge forPath(String path, byte[] data) throws Exception
            {
                if ( compress )
                {
                    data = client.getCompressionProvider().compress(path, data);
                }

                String      fixedPath = client.fixForNamespace(path);
                transaction.add(Op.setData(fixedPath, data, version), OperationType.SET_DATA, path);
                return curatorTransaction;
            }

            @Override
            public CuratorTransactionBridge forPath(String path) throws Exception
            {
                return forPath(path, client.getDefaultData());
            }

            @Override
            public PathAndBytesable<CuratorTransactionBridge> withVersion(int version)
            {
                SetDataBuilderImpl.this.withVersion(version);
                return this;
            }

            @Override
            public VersionPathAndBytesable<CuratorTransactionBridge> compressed() {
                compress = true;

                return this;
            }
        };
    }

    @Override
    public SetDataBackgroundVersionable compressed()
    {
        compress = true;
        return new SetDataBackgroundVersionable()
        {
            @Override
            public ErrorListenerPathAndBytesable<Stat> inBackground()
            {
                return SetDataBuilderImpl.this.inBackground();
            }

            @Override
            public ErrorListenerPathAndBytesable<Stat> inBackground(BackgroundCallback callback, Object context)
            {
                return SetDataBuilderImpl.this.inBackground(callback, context);
            }

            @Override
            public ErrorListenerPathAndBytesable<Stat> inBackground(BackgroundCallback callback, Object context, Executor executor)
            {
                return SetDataBuilderImpl.this.inBackground(callback, context, executor);
            }

            @Override
            public ErrorListenerPathAndBytesable<Stat> inBackground(Object context)
            {
                return SetDataBuilderImpl.this.inBackground(context);
            }

            @Override
            public ErrorListenerPathAndBytesable<Stat> inBackground(BackgroundCallback callback)
            {
                return SetDataBuilderImpl.this.inBackground(callback);
            }

            @Override
            public ErrorListenerPathAndBytesable<Stat> inBackground(BackgroundCallback callback, Executor executor)
            {
                return SetDataBuilderImpl.this.inBackground(callback, executor);
            }

            @Override
            public Stat forPath(String path, byte[] data) throws Exception
            {
                return SetDataBuilderImpl.this.forPath(path, data);
            }

            @Override
            public Stat forPath(String path) throws Exception
            {
                return SetDataBuilderImpl.this.forPath(path);
            }

            @Override
            public BackgroundPathAndBytesable<Stat> withVersion(int version)
            {
                return SetDataBuilderImpl.this.withVersion(version);
            }
        };
    }

    @Override
    public BackgroundPathAndBytesable<Stat> withVersion(int version)
    {
        this.version = version;
        return this;
    }

    @Override
    public ErrorListenerPathAndBytesable<Stat> inBackground(BackgroundCallback callback, Object context)
    {
        backgrounding = new Backgrounding(callback, context);
        return this;
    }

    @Override
    public ErrorListenerPathAndBytesable<Stat> inBackground(BackgroundCallback callback, Object context, Executor executor)
    {
        backgrounding = new Backgrounding(client, callback, context, executor);
        return this;
    }

    @Override
    public ErrorListenerPathAndBytesable<Stat> inBackground(BackgroundCallback callback)
    {
        backgrounding = new Backgrounding(callback);
        return this;
    }

    @Override
    public ErrorListenerPathAndBytesable<Stat> inBackground()
    {
        backgrounding = new Backgrounding(true);
        return this;
    }

    @Override
    public ErrorListenerPathAndBytesable<Stat> inBackground(Object context)
    {
        backgrounding = new Backgrounding(context);
        return this;
    }

    @Override
    public ErrorListenerPathAndBytesable<Stat> inBackground(BackgroundCallback callback, Executor executor)
    {
        backgrounding = new Backgrounding(client, callback, executor);
        return this;
    }

    @Override
    public PathAndBytesable<Stat> withUnhandledErrorListener(UnhandledErrorListener listener)
    {
        backgrounding = new Backgrounding(backgrounding, listener);
        return this;
    }

    @Override
    public void performBackgroundOperation(final OperationAndData<PathAndBytes> operationAndData) throws Exception
    {
        try
        {
            final OperationTrace   trace = client.getZookeeperClient().startTracer("SetDataBuilderImpl-Background");
            client.getZooKeeper().setData
            (
                operationAndData.getData().getPath(),
                operationAndData.getData().getData(),
                version,
                new AsyncCallback.StatCallback()
                {
                    @SuppressWarnings({"unchecked"})
                    @Override
                    public void processResult(int rc, String path, Object ctx, Stat stat)
                    {
                        trace.setRequestBytesLength(operationAndData.getData().getData().length).commit();
                        CuratorEvent event = new CuratorEventImpl(client, CuratorEventType.SET_DATA, rc, path, null, ctx, stat, null, null, null, null);
                        client.processBackgroundOperation(operationAndData, event);
                    }
                },
                backgrounding.getContext()
            );
        }
        catch ( Throwable e )
        {
            backgrounding.checkError(e);
        }
    }

    @Override
    public Stat forPath(String path) throws Exception
    {
        return forPath(path, client.getDefaultData());
    }

    @Override
    public Stat forPath(String path, byte[] data) throws Exception
    {
        if ( compress )
        {
            data = client.getCompressionProvider().compress(path, data);
        }

        path = client.fixForNamespace(path);

        Stat        resultStat = null;
        if ( backgrounding.inBackground()  )
        {
            client.processBackgroundOperation(new OperationAndData<PathAndBytes>(this, new PathAndBytes(path, data), backgrounding.getCallback(), null, backgrounding.getContext()), null);
        }
        else
        {
            resultStat = pathInForeground(path, data);
        }
        return resultStat;
    }

    int getVersion()
    {
        return version;
    }

    private Stat pathInForeground(final String path, final byte[] data) throws Exception
    {
        OperationTrace   trace = client.getZookeeperClient().startTracer("SetDataBuilderImpl-Foreground");
        Stat        resultStat = RetryLoop.callWithRetry
        (
            client.getZookeeperClient(),
            new Callable<Stat>()
            {
                @Override
                public Stat call() throws Exception
                {
                    return client.getZooKeeper().setData(path, data, version);
                }
            }
        );
        trace.setRequestBytesLength(data.length).commit();
        return resultStat;
    }
}
