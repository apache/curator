/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.curator.framework.imps;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.netflix.curator.RetryLoop;
import com.netflix.curator.framework.api.transaction.CuratorTransaction;
import com.netflix.curator.framework.api.transaction.CuratorTransactionResult;
import com.netflix.curator.framework.api.transaction.OperationType;
import com.netflix.curator.framework.api.transaction.TransactionCreateBuilder;
import com.netflix.curator.framework.api.transaction.TransactionDeleteBuilder;
import com.netflix.curator.framework.api.transaction.TransactionSetDataBuilder;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;

class CuratorTransactionImpl implements CuratorTransaction
{
    private final CuratorFrameworkImpl              client;
    private final CuratorMultiTransactionRecord     transaction;

    private boolean         isCommitted = false;

    CuratorTransactionImpl(CuratorFrameworkImpl client)
    {
        this.client = client;
        transaction = new CuratorMultiTransactionRecord();
    }

    @Override
    public TransactionCreateBuilder create()
    {
        Preconditions.checkArgument(!isCommitted);

        return new CreateBuilderImpl(client).asTransactionCreateBuilder(this, transaction);
    }

    @Override
    public TransactionDeleteBuilder delete()
    {
        Preconditions.checkArgument(!isCommitted);

        return new DeleteBuilderImpl(client).asTransactionDeleteBuilder(this, transaction);
    }

    @Override
    public TransactionSetDataBuilder setData()
    {
        Preconditions.checkArgument(!isCommitted);

        return new SetDataBuilderImpl(client).asTransactionSetDataBuilder(this, transaction);
    }

    @Override
    public void check(String path, int version)
    {
        Preconditions.checkArgument(!isCommitted);

        path = client.fixForNamespace(path);
        transaction.add(Op.check(path, version), OperationType.CHECK, path);
    }

    @Override
    public Collection<CuratorTransactionResult> commit() throws Exception
    {
        Preconditions.checkArgument(!isCommitted);
        isCommitted = true;

        List<OpResult>      resultList = RetryLoop.callWithRetry
        (
            client.getZookeeperClient(),
            new Callable<List<OpResult>>()
            {
                @Override
                public List<OpResult> call() throws Exception
                {
                    return doOperation();
                }
            }
        );
        
        if ( resultList.size() != transaction.metadataSize() )
        {
            throw new IllegalStateException(String.format("Result size (%d) doesn't match input size (%d)", resultList.size(), transaction.metadataSize()));
        }

        ImmutableList.Builder<CuratorTransactionResult>     builder = ImmutableList.builder();
        for ( int i = 0; i < resultList.size(); ++i )
        {
            OpResult                                    opResult = resultList.get(i);
            CuratorMultiTransactionRecord.TypeAndPath   metadata = transaction.getMetadata(i);
            CuratorTransactionResult                    curatorResult = makeCuratorResult(opResult, metadata);
            builder.add(curatorResult);
        }

        return builder.build();
    }

    private List<OpResult> doOperation() throws Exception
    {
        List<OpResult>  opResults = client.getZooKeeper().multi(transaction);
        if ( opResults.size() > 0 )
        {
            OpResult        firstResult = opResults.get(0);
            if ( firstResult.getType() == ZooDefs.OpCode.error )
            {
                OpResult.ErrorResult        error = (OpResult.ErrorResult)firstResult;
                KeeperException.Code        code = KeeperException.Code.get(error.getErr());
                if ( code == null )
                {
                    code = KeeperException.Code.UNIMPLEMENTED;
                }
                throw KeeperException.create(code);
            }
        }
        return opResults;
    }

    private CuratorTransactionResult makeCuratorResult(OpResult opResult, CuratorMultiTransactionRecord.TypeAndPath metadata)
    {
        String                                      resultPath = null;
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
                OpResult.CreateResult       createResult = (OpResult.CreateResult)opResult;
                resultPath = createResult.getPath();
                break;
            }

            case ZooDefs.OpCode.setData:
            {
                OpResult.SetDataResult      setDataResult = (OpResult.SetDataResult)opResult;
                resultStat = setDataResult.getStat();
                break;
            }
        }

        return new CuratorTransactionResult(metadata.type, metadata.forPath, resultPath, resultStat);
    }
}
