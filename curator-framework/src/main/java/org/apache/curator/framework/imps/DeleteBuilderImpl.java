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
import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.curator.framework.api.DeleteBuilderBase;
import org.apache.curator.framework.api.Pathable;
import org.apache.curator.framework.api.transaction.CuratorTransactionBridge;
import org.apache.curator.framework.api.transaction.OperationType;
import org.apache.curator.framework.api.transaction.TransactionDeleteBuilder;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

class DeleteBuilderImpl implements DeleteBuilder, BackgroundOperation<String>
{
    private final CuratorFrameworkImpl  client;
    private int                         version;
    private Backgrounding               backgrounding;
    private boolean                     guaranteed;

    DeleteBuilderImpl(CuratorFrameworkImpl client)
    {
        this.client = client;
        version = -1;
        backgrounding = new Backgrounding();
        guaranteed = false;
    }

    TransactionDeleteBuilder    asTransactionDeleteBuilder(final CuratorTransactionImpl curatorTransaction, final CuratorMultiTransactionRecord transaction)
    {
        return new TransactionDeleteBuilder()
        {
            @Override
            public CuratorTransactionBridge forPath(String path) throws Exception
            {
                String      fixedPath = client.fixForNamespace(path);
                transaction.add(Op.delete(fixedPath, version), OperationType.DELETE, path);
                return curatorTransaction;
            }

            @Override
            public Pathable<CuratorTransactionBridge> withVersion(int version)
            {
                DeleteBuilderImpl.this.withVersion(version);
                return this;
            }
        };
    }

    @Override
    public DeleteBuilderBase guaranteed()
    {
        guaranteed = true;
        return this;
    }

    @Override
    public BackgroundPathable<Void> withVersion(int version)
    {
        this.version = version;
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
    public void performBackgroundOperation(final OperationAndData<String> operationAndData) throws Exception
    {
        final TimeTrace   trace = client.getZookeeperClient().startTracer("DeleteBuilderImpl-Background");
        client.getZooKeeper().delete
        (
            operationAndData.getData(),
            version,
            new AsyncCallback.VoidCallback()
            {
                @Override
                public void processResult(int rc, String path, Object ctx)
                {
                    trace.commit();
                    CuratorEvent event = new CuratorEventImpl(client, CuratorEventType.DELETE, rc, path, null, ctx, null, null, null, null, null);
                    client.processBackgroundOperation(operationAndData, event);
                }
            },
            backgrounding.getContext()
        );
    }

    @Override
    public Void forPath(String path) throws Exception
    {
        final String        unfixedPath = path;
        path = client.fixForNamespace(path);

        if ( backgrounding.inBackground() )
        {
            OperationAndData.ErrorCallback<String>  errorCallback = null;
            if ( guaranteed )
            {
                errorCallback = new OperationAndData.ErrorCallback<String>()
                {
                    @Override
                    public void retriesExhausted(OperationAndData<String> operationAndData)
                    {
                        client.getFailedDeleteManager().addFailedDelete(unfixedPath);
                    }
                };
            }
            client.processBackgroundOperation(new OperationAndData<String>(this, path, backgrounding.getCallback(), errorCallback), null);
        }
        else
        {
            pathInForeground(path, unfixedPath);
        }
        return null;
    }

    protected int getVersion()
    {
        return version;
    }

    private void pathInForeground(final String path, String unfixedPath) throws Exception
    {
        TimeTrace       trace = client.getZookeeperClient().startTracer("DeleteBuilderImpl-Foreground");
        try
        {
            RetryLoop.callWithRetry
            (
                client.getZookeeperClient(),
                new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        client.getZooKeeper().delete(path, version);
                        return null;
                    }
                }
            );
        }
        catch ( KeeperException.NodeExistsException e )
        {
            throw e;
        }
        catch ( Exception e )
        {
            if ( guaranteed )
            {
                client.getFailedDeleteManager().addFailedDelete(unfixedPath);
            }
            throw e;
        }
        trace.commit();
    }
}
