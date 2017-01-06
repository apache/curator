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
package org.apache.curator.x.async.details;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.imps.CuratorFrameworkImpl;
import org.apache.curator.framework.imps.CuratorMultiTransactionImpl;
import org.apache.curator.framework.imps.GetACLBuilderImpl;
import org.apache.curator.framework.imps.SyncBuilderImpl;
import org.apache.curator.x.async.*;
import org.apache.curator.x.async.api.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import java.util.List;
import java.util.Objects;

import static org.apache.curator.x.async.details.BackgroundProcs.*;

public class AsyncCuratorFrameworkImpl implements AsyncCuratorFramework
{
    private final CuratorFrameworkImpl client;
    private final UnhandledErrorListener unhandledErrorListener;
    private final boolean watched;

    public AsyncCuratorFrameworkImpl(CuratorFramework client)
    {
        this(reveal(client), null, false);
    }

    private static CuratorFrameworkImpl reveal(CuratorFramework client)
    {
        try
        {
            return (CuratorFrameworkImpl)Objects.requireNonNull(client, "client cannot be null");
        }
        catch ( Exception e )
        {
            throw new IllegalArgumentException("Only Curator clients created through CuratorFrameworkFactory are supported: " + client.getClass().getName());
        }
    }

    public AsyncCuratorFrameworkImpl(CuratorFrameworkImpl client, UnhandledErrorListener unhandledErrorListener, boolean watched)
    {
        this.client = client;
        this.unhandledErrorListener = unhandledErrorListener;
        this.watched = watched;
    }

    @Override
    public AsyncCreateBuilder create()
    {
        return new AsyncCreateBuilderImpl(client, unhandledErrorListener);
    }

    @Override
    public AsyncDeleteBuilder delete()
    {
        return new AsyncDeleteBuilderImpl(client, unhandledErrorListener);
    }

    @Override
    public AsyncSetDataBuilder setData()
    {
        return new AsyncSetDataBuilderImpl(client, unhandledErrorListener);
    }

    @Override
    public AsyncGetACLBuilder getACL()
    {
        return new AsyncGetACLBuilder()
        {
            private Stat stat = null;

            @Override
            public AsyncPathable<AsyncStage<List<ACL>>> storingStatIn(Stat stat)
            {
                this.stat = stat;
                return this;
            }

            @Override
            public AsyncStage<List<ACL>> forPath(String path)
            {
                BuilderCommon<List<ACL>> common = new BuilderCommon<>(unhandledErrorListener, false, aclProc);
                GetACLBuilderImpl builder = new GetACLBuilderImpl(client, common.backgrounding, stat);
                return safeCall(common.internalCallback, () -> builder.forPath(path));
            }
        };
    }

    @Override
    public AsyncSetACLBuilder setACL()
    {
        return new AsyncSetACLBuilderImpl(client, unhandledErrorListener);
    }

    @Override
    public AsyncReconfigBuilder reconfig()
    {
        return new AsyncReconfigBuilderImpl(client, unhandledErrorListener);
    }

    @Override
    public AsyncMultiTransaction transaction()
    {
        return operations -> {
            BuilderCommon<List<CuratorTransactionResult>> common = new BuilderCommon<>(unhandledErrorListener, false, opResultsProc);
            CuratorMultiTransactionImpl builder = new CuratorMultiTransactionImpl(client, common.backgrounding);
            return safeCall(common.internalCallback, () -> builder.forOperations(operations));
        };
    }

    @Override
    public AsyncSyncBuilder sync()
    {
        return path -> {
            BuilderCommon<Void> common = new BuilderCommon<>(unhandledErrorListener, false, ignoredProc);
            SyncBuilderImpl builder = new SyncBuilderImpl(client, common.backgrounding);
            return safeCall(common.internalCallback, () -> builder.forPath(path));
        };
    }

    @Override
    public AsyncRemoveWatchesBuilder removeWatches()
    {
        return new AsyncRemoveWatchesBuilderImpl(client, unhandledErrorListener);
    }

    @Override
    public CuratorFramework unwrap()
    {
        return client;
    }

    @Override
    public WatchedAsyncCuratorFramework watched()
    {
        return new AsyncCuratorFrameworkImpl(client, unhandledErrorListener, true);
    }

    @Override
    public AsyncCuratorFrameworkDsl withUnhandledErrorListener(UnhandledErrorListener listener)
    {
        return new AsyncCuratorFrameworkImpl(client, listener, watched);
    }

    @Override
    public AsyncTransactionOp transactionOp()
    {
        return new AsyncTransactionOpImpl(client);
    }

    @Override
    public AsyncExistsBuilder checkExists()
    {
        return new AsyncExistsBuilderImpl(client, unhandledErrorListener, watched);
    }

    @Override
    public AsyncGetDataBuilder getData()
    {
        return new AsyncGetDataBuilderImpl(client, unhandledErrorListener, watched);
    }

    @Override
    public AsyncGetChildrenBuilder getChildren()
    {
        return new AsyncGetChildrenBuilderImpl(client, unhandledErrorListener, watched);
    }

    @Override
    public AsyncGetConfigBuilder getConfig()
    {
        return new AsyncGetConfigBuilderImpl(client, unhandledErrorListener, watched);
    }
}
