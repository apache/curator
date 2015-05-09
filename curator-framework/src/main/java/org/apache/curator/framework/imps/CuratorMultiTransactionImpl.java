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
import org.apache.curator.framework.api.transaction.CuratorMultiTransaction;
import org.apache.curator.framework.api.transaction.CuratorMultiTransactionMain;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

public class CuratorMultiTransactionImpl implements
    CuratorMultiTransaction,
    CuratorMultiTransactionMain,
    BackgroundOperation<Void>
{
    private final CuratorFrameworkImpl client;
    private Backgrounding backgrounding = new Backgrounding();

    public CuratorMultiTransactionImpl(CuratorFrameworkImpl client)
    {
        this.client = client;
    }

    @Override
    public CuratorMultiTransactionMain inBackground()
    {
        backgrounding = new Backgrounding(true);
        return this;
    }

    @Override
    public CuratorMultiTransactionMain inBackground(Object context)
    {
        backgrounding = new Backgrounding(context);
        return this;
    }

    @Override
    public CuratorMultiTransactionMain inBackground(BackgroundCallback callback)
    {
        backgrounding = new Backgrounding(callback);
        return this;
    }

    @Override
    public CuratorMultiTransactionMain inBackground(BackgroundCallback callback, Object context)
    {
        backgrounding = new Backgrounding(callback, context);
        return this;
    }

    @Override
    public CuratorMultiTransactionMain inBackground(BackgroundCallback callback, Executor executor)
    {
        backgrounding = new Backgrounding(callback, executor);
        return this;
    }

    @Override
    public CuratorMultiTransactionMain inBackground(BackgroundCallback callback, Object context, Executor executor)
    {
        backgrounding = new Backgrounding(client, callback, context, executor);
        return this;
    }

    @Override
    public List<OpResult> forOperations(Op... operations) throws Exception
    {
        List<Op> ops = (operations != null) ? Arrays.asList(operations) : Lists.<Op>newArrayList();
        return forOperations(ops);
    }

    @Override
    public List<OpResult> forOperations(List<Op> operations) throws Exception
    {
        operations = Preconditions.checkNotNull(operations, "operations cannot be null");
        Preconditions.checkArgument(!operations.isEmpty(), "operations list cannot be empty");

        if ( backgrounding.inBackground() )
        {
            client.processBackgroundOperation(new OperationAndData<>(this, null, backgrounding.getCallback(), null, backgrounding.getContext()), null);
            return null;
        }
        else
        {
            return forOperationsInForeground(operations);
        }
    }

    @Override
    public void performBackgroundOperation(OperationAndData<Void> data) throws Exception
    {

    }

    private List<OpResult> forOperationsInForeground(final List<Op> operations) throws Exception
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
                    return client.getZooKeeper().multi(operations);
                }
            }
        );
        trace.commit();

        return responseData;
    }
}
