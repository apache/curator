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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.curator.RetryLoop;
import org.apache.curator.TimeTrace;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.ErrorListenerMultiTransactionMain;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.api.transaction.CuratorMultiTransaction;
import org.apache.curator.framework.api.transaction.CuratorMultiTransactionMain;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.schema.Schema;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.SetDataRequest;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

public class CuratorMultiTransactionImpl implements
    CuratorMultiTransaction,
    CuratorMultiTransactionMain,
    BackgroundOperation<CuratorMultiTransactionRecord>,
    ErrorListenerMultiTransactionMain
{
    private final CuratorFrameworkImpl client;
    private Backgrounding backgrounding = new Backgrounding();

    public CuratorMultiTransactionImpl(CuratorFrameworkImpl client)
    {
        this.client = client;
    }

    public CuratorMultiTransactionImpl(CuratorFrameworkImpl client, Backgrounding backgrounding)
    {
        this.client = client;
        this.backgrounding = backgrounding;
    }

    @Override
    public ErrorListenerMultiTransactionMain inBackground()
    {
        backgrounding = new Backgrounding(true);
        return this;
    }

    @Override
    public ErrorListenerMultiTransactionMain inBackground(Object context)
    {
        backgrounding = new Backgrounding(context);
        return this;
    }

    @Override
    public ErrorListenerMultiTransactionMain inBackground(BackgroundCallback callback)
    {
        backgrounding = new Backgrounding(callback);
        return this;
    }

    @Override
    public ErrorListenerMultiTransactionMain inBackground(BackgroundCallback callback, Object context)
    {
        backgrounding = new Backgrounding(callback, context);
        return this;
    }

    @Override
    public ErrorListenerMultiTransactionMain inBackground(BackgroundCallback callback, Executor executor)
    {
        backgrounding = new Backgrounding(callback, executor);
        return this;
    }

    @Override
    public ErrorListenerMultiTransactionMain inBackground(BackgroundCallback callback, Object context, Executor executor)
    {
        backgrounding = new Backgrounding(client, callback, context, executor);
        return this;
    }

    @Override
    public CuratorMultiTransactionMain withUnhandledErrorListener(UnhandledErrorListener listener)
    {
        backgrounding = new Backgrounding(backgrounding, listener);
        return this;
    }

    @Override
    public List<CuratorTransactionResult> forOperations(CuratorOp... operations) throws Exception
    {
        List<CuratorOp> ops = (operations != null) ? Arrays.asList(operations) : Lists.<CuratorOp>newArrayList();
        return forOperations(ops);
    }

    @Override
    public List<CuratorTransactionResult> forOperations(List<CuratorOp> operations) throws Exception
    {
        operations = Preconditions.checkNotNull(operations, "operations cannot be null");
        Preconditions.checkArgument(!operations.isEmpty(), "operations list cannot be empty");

        CuratorMultiTransactionRecord record = new CuratorMultiTransactionRecord();
        for ( CuratorOp curatorOp : operations )
        {
            Schema schema = client.getSchemaSet().getSchema(curatorOp.getTypeAndPath().getForPath());
            record.add(curatorOp.get(), curatorOp.getTypeAndPath().getType(), curatorOp.getTypeAndPath().getForPath());
            if ( (curatorOp.get().getType() == ZooDefs.OpCode.create) || (curatorOp.get().getType() == ZooDefs.OpCode.createContainer) )
            {
                CreateRequest createRequest = (CreateRequest)curatorOp.get().toRequestRecord();
                CreateMode createMode = CreateMode.fromFlag(createRequest.getFlags(), CreateMode.PERSISTENT);
                schema.validateCreate(createMode, createRequest.getPath(), createRequest.getData(), createRequest.getAcl());
            }
            else if ( (curatorOp.get().getType() == ZooDefs.OpCode.delete) || (curatorOp.get().getType() == ZooDefs.OpCode.deleteContainer) )
            {
                DeleteRequest deleteRequest = (DeleteRequest)curatorOp.get().toRequestRecord();
                schema.validateDelete(deleteRequest.getPath());
            }
            else if ( curatorOp.get().getType() == ZooDefs.OpCode.setData )
            {
                SetDataRequest setDataRequest = (SetDataRequest)curatorOp.get().toRequestRecord();
                schema.validateGeneral(setDataRequest.getPath(), setDataRequest.getData(), null);
            }
        }

        if ( backgrounding.inBackground() )
        {
            client.processBackgroundOperation(new OperationAndData<>(this, record, backgrounding.getCallback(), null, backgrounding.getContext(), null), null);
            return null;
        }
        else
        {
            return forOperationsInForeground(record);
        }
    }

    @Override
    public void performBackgroundOperation(final OperationAndData<CuratorMultiTransactionRecord> operationAndData) throws Exception
    {
        try
        {
            final TimeTrace trace = client.getZookeeperClient().startTracer("CuratorMultiTransactionImpl-Background");
            AsyncCallback.MultiCallback callback = new AsyncCallback.MultiCallback()
            {
                @Override
                public void processResult(int rc, String path, Object ctx, List<OpResult> opResults)
                {
                    trace.commit();
                    List<CuratorTransactionResult> curatorResults = (opResults != null) ? CuratorTransactionImpl.wrapResults(client, opResults, operationAndData.getData()) : null;
                    CuratorEvent event = new CuratorEventImpl(client, CuratorEventType.TRANSACTION, rc, path, null, ctx, null, null, null, null, null, curatorResults);
                    client.processBackgroundOperation(operationAndData, event);
                }
            };
            client.getZooKeeper().multi(operationAndData.getData(), callback, backgrounding.getContext());
        }
        catch ( Throwable e )
        {
            backgrounding.checkError(e, null);
        }
    }

    private List<CuratorTransactionResult> forOperationsInForeground(final CuratorMultiTransactionRecord record) throws Exception
    {
        TimeTrace trace = client.getZookeeperClient().startTracer("CuratorMultiTransactionImpl-Foreground");
        List<OpResult> responseData = RetryLoop.callWithRetry
        (
            client.getZookeeperClient(),
            new Callable<List<OpResult>>()
            {
                @Override
                public List<OpResult> call() throws Exception
                {
                    return client.getZooKeeper().multi(record);
                }
            }
        );
        trace.commit();

        return CuratorTransactionImpl.wrapResults(client, responseData, record);
    }
}
