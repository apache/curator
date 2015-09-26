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
import com.google.common.collect.ImmutableList;
import org.apache.curator.RetryLoop;
import org.apache.curator.framework.api.Pathable;
import org.apache.curator.framework.api.transaction.*;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

class CuratorTransactionImpl implements CuratorTransaction, CuratorTransactionBridge, CuratorTransactionFinal
{
    private final CuratorFrameworkImpl client;
    private final CuratorMultiTransactionRecord transaction;

    private boolean isCommitted = false;

    CuratorTransactionImpl(CuratorFrameworkImpl client)
    {
        this.client = client;
        transaction = new CuratorMultiTransactionRecord();
    }

    @Override
    public CuratorTransactionFinal and()
    {
        return this;
    }

    @Override
    public TransactionCreateBuilder<CuratorTransactionBridge> create()
    {
        Preconditions.checkState(!isCommitted, "transaction already committed");

        CuratorTransactionBridge asBridge = this;
        return new CreateBuilderImpl(client).asTransactionCreateBuilder(asBridge, transaction);
    }

    @Override
    public TransactionDeleteBuilder<CuratorTransactionBridge> delete()
    {
        Preconditions.checkState(!isCommitted, "transaction already committed");

        CuratorTransactionBridge asBridge = this;
        return new DeleteBuilderImpl(client).asTransactionDeleteBuilder(asBridge, transaction);
    }

    @Override
    public TransactionSetDataBuilder<CuratorTransactionBridge> setData()
    {
        Preconditions.checkState(!isCommitted, "transaction already committed");

        CuratorTransactionBridge asBridge = this;
        return new SetDataBuilderImpl(client).asTransactionSetDataBuilder(asBridge, transaction);
    }

    @Override
    public TransactionCheckBuilder<CuratorTransactionBridge> check()
    {
        Preconditions.checkState(!isCommitted, "transaction already committed");

        CuratorTransactionBridge asBridge = this;
        return makeTransactionCheckBuilder(client, asBridge, transaction);
    }

    static <T> TransactionCheckBuilder<T> makeTransactionCheckBuilder(final CuratorFrameworkImpl client, final T context, final CuratorMultiTransactionRecord transaction)
    {
        return new TransactionCheckBuilder<T>()
        {
            private int version = -1;

            @Override
            public T forPath(String path) throws Exception
            {
                String fixedPath = client.fixForNamespace(path);
                transaction.add(Op.check(fixedPath, version), OperationType.CHECK, path);

                return context;
            }

            @Override
            public Pathable<T> withVersion(int version)
            {
                this.version = version;
                return this;
            }
        };
    }

    @Override
    public Collection<CuratorTransactionResult> commit() throws Exception
    {
        Preconditions.checkState(!isCommitted, "transaction already committed");
        isCommitted = true;

        final AtomicBoolean firstTime = new AtomicBoolean(true);
        List<OpResult> resultList = RetryLoop.callWithRetry
            (
                client.getZookeeperClient(),
                new Callable<List<OpResult>>()
                {
                    @Override
                    public List<OpResult> call() throws Exception
                    {
                        return doOperation(firstTime);
                    }
                }
            );

        if ( resultList.size() != transaction.metadataSize() )
        {
            throw new IllegalStateException(String.format("Result size (%d) doesn't match input size (%d)", resultList.size(), transaction.metadataSize()));
        }

        return wrapResults(client, resultList, transaction);
    }

    static List<CuratorTransactionResult> wrapResults(CuratorFrameworkImpl client, List<OpResult> resultList, CuratorMultiTransactionRecord transaction)
    {
        ImmutableList.Builder<CuratorTransactionResult> builder = ImmutableList.builder();
        for ( int i = 0; i < resultList.size(); ++i )
        {
            OpResult opResult = resultList.get(i);
            TypeAndPath metadata = transaction.getMetadata(i);
            CuratorTransactionResult curatorResult = makeCuratorResult(client, opResult, metadata);
            builder.add(curatorResult);
        }

        return builder.build();
    }

    static CuratorTransactionResult makeCuratorResult(CuratorFrameworkImpl client, OpResult opResult, TypeAndPath metadata)
    {
        String resultPath = null;
        Stat resultStat = null;
        switch ( opResult.getType() )
        {
            default:
            {
                // NOP
                break;
            }

            case ZooDefs.OpCode.create:
            {
                OpResult.CreateResult createResult = (OpResult.CreateResult)opResult;
                resultPath = client.unfixForNamespace(createResult.getPath());
                break;
            }

            case ZooDefs.OpCode.setData:
            {
                OpResult.SetDataResult setDataResult = (OpResult.SetDataResult)opResult;
                resultStat = setDataResult.getStat();
                break;
            }
        }

        return new CuratorTransactionResult(metadata.getType(), metadata.getForPath(), resultPath, resultStat);
    }

    private List<OpResult> doOperation(AtomicBoolean firstTime) throws Exception
    {
        boolean localFirstTime = firstTime.getAndSet(false);
        if ( !localFirstTime )
        {
            // TODO
        }

        List<OpResult> opResults = client.getZooKeeper().multi(transaction);
        if ( opResults.size() > 0 )
        {
            OpResult firstResult = opResults.get(0);
            if ( firstResult.getType() == ZooDefs.OpCode.error )
            {
                OpResult.ErrorResult error = (OpResult.ErrorResult)firstResult;
                KeeperException.Code code = KeeperException.Code.get(error.getErr());
                if ( code == null )
                {
                    code = KeeperException.Code.UNIMPLEMENTED;
                }
                throw KeeperException.create(code);
            }
        }
        return opResults;
    }
}
