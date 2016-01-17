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
import org.apache.curator.framework.api.BackgroundVersionable;
import org.apache.curator.framework.api.ChildrenDeletable;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.curator.framework.api.DeleteBuilderMain;
import org.apache.curator.framework.api.Pathable;
import org.apache.curator.framework.api.transaction.OperationType;
import org.apache.curator.framework.api.transaction.TransactionDeleteBuilder;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

class DeleteBuilderImpl implements DeleteBuilder, BackgroundOperation<String>
{
    private final CuratorFrameworkImpl client;
    private int version;
    private Backgrounding backgrounding;
    private boolean deletingChildrenIfNeeded;
    private boolean guaranteed;
    private boolean quietly;

    DeleteBuilderImpl(CuratorFrameworkImpl client)
    {
        this.client = client;
        version = -1;
        backgrounding = new Backgrounding();
        deletingChildrenIfNeeded = false;
        guaranteed = false;
        quietly = false;
    }

    <T> TransactionDeleteBuilder<T> asTransactionDeleteBuilder(final T context, final CuratorMultiTransactionRecord transaction)
    {
        return new TransactionDeleteBuilder<T>()
        {
            @Override
            public T forPath(String path) throws Exception
            {
                String fixedPath = client.fixForNamespace(path);
                transaction.add(Op.delete(fixedPath, version), OperationType.DELETE, path);
                return context;
            }

            @Override
            public Pathable<T> withVersion(int version)
            {
                DeleteBuilderImpl.this.withVersion(version);
                return this;
            }
        };
    }

    @Override
    public DeleteBuilderMain quietly()
    {
        quietly = true;
        return this;
    }

    @Override
    public ChildrenDeletable guaranteed()
    {
        guaranteed = true;
        return this;
    }

    @Override
    public BackgroundVersionable deletingChildrenIfNeeded()
    {
        deletingChildrenIfNeeded = true;
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
        final TimeTrace trace = client.getZookeeperClient().startTracer("DeleteBuilderImpl-Background");
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
                        if ( (rc == KeeperException.Code.NOTEMPTY.intValue()) && deletingChildrenIfNeeded )
                        {
                            backgroundDeleteChildrenThenNode(operationAndData);
                        }
                        else
                        {
                            if ( (rc == KeeperException.Code.NONODE.intValue()) && quietly )
                            {
                                rc = KeeperException.Code.OK.intValue();
                            }
                            CuratorEvent event = new CuratorEventImpl(client, CuratorEventType.DELETE, rc, path, null, ctx, null, null, null, null, null, null);
                            client.processBackgroundOperation(operationAndData, event);
                        }
                    }
                },
                backgrounding.getContext()
            );
    }

    private void backgroundDeleteChildrenThenNode(final OperationAndData<String> mainOperationAndData)
    {
        BackgroundOperation<String> operation = new BackgroundOperation<String>()
        {
            @Override
            public void performBackgroundOperation(OperationAndData<String> dummy) throws Exception
            {
                try
                {
                    ZKPaths.deleteChildren(client.getZooKeeper(), mainOperationAndData.getData(), false);
                }
                catch ( KeeperException e )
                {
                    // ignore
                }
                client.queueOperation(mainOperationAndData);
            }
        };
        OperationAndData<String> parentOperation = new OperationAndData<String>(operation, mainOperationAndData.getData(), null, null, backgrounding.getContext());
        client.queueOperation(parentOperation);
    }

    @Override
    public Void forPath(String path) throws Exception
    {
        final String unfixedPath = path;
        path = client.fixForNamespace(path);

        if ( backgrounding.inBackground() )
        {
            OperationAndData.ErrorCallback<String> errorCallback = null;
            if ( guaranteed )
            {
                errorCallback = new OperationAndData.ErrorCallback<String>()
                {
                    @Override
                    public void retriesExhausted(OperationAndData<String> operationAndData)
                    {
                        client.getFailedDeleteManager().addFailedOperation(unfixedPath);
                    }
                };
            }
            client.processBackgroundOperation(new OperationAndData<String>(this, path, backgrounding.getCallback(), errorCallback, backgrounding.getContext()), null);
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
        TimeTrace trace = client.getZookeeperClient().startTracer("DeleteBuilderImpl-Foreground");
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
                            try
                            {
                                client.getZooKeeper().delete(path, version);
                            }
                            catch ( KeeperException.NoNodeException e )
                            {
                                if ( !quietly )
                                {
                                    throw e;
                                }
                            }
                            catch ( KeeperException.NotEmptyException e )
                            {
                                if ( deletingChildrenIfNeeded )
                                {
                                    ZKPaths.deleteChildren(client.getZooKeeper(), path, true);
                                }
                                else
                                {
                                    throw e;
                                }
                            }
                            return null;
                        }
                    }
                );
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            //Only retry a guaranteed delete if it's a retryable error
            if( (RetryLoop.isRetryException(e) || (e instanceof InterruptedException)) && guaranteed )
            {
                client.getFailedDeleteManager().addFailedOperation(unfixedPath);
            }
            throw e;
        }
        trace.commit();
    }
}
