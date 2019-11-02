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
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.v2.AsyncCuratorFrameworkV2;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.WatchMode;
import org.apache.curator.x.async.api.*;
import org.apache.zookeeper.WatchedEvent;
import java.util.Objects;
import java.util.function.UnaryOperator;

public class AsyncCuratorFrameworkV2Impl implements AsyncCuratorFrameworkV2
{
    private final AsyncCuratorFrameworkImpl client;

    public AsyncCuratorFrameworkV2Impl(AsyncCuratorFramework client)
    {
        this(reveal(client));
    }

    private AsyncCuratorFrameworkV2Impl(AsyncCuratorFrameworkImpl client)
    {
        this.client = client;
    }

    private static AsyncCuratorFrameworkImpl reveal(Object client)
    {
        try
        {
            return (AsyncCuratorFrameworkImpl)Objects.requireNonNull(client, "client cannot be null");
        }
        catch ( Exception e )
        {
            throw new IllegalArgumentException("Only AsyncCuratorFramework clients wrapped via AsyncCuratorFramework.wrap(): " + client.getClass().getName());
        }
    }

    @Override
    public CuratorFramework unwrap()
    {
        return client.unwrap();
    }

    @Override
    public AsyncCuratorFrameworkDslV2 with(WatchMode mode)
    {
        return new AsyncCuratorFrameworkV2Impl(reveal(client.with(mode)));
    }

    @Override
    public AsyncCuratorFrameworkDslV2 with(UnhandledErrorListener listener)
    {
        return new AsyncCuratorFrameworkV2Impl(reveal(client.with(listener)));
    }

    @Override
    public AsyncCuratorFrameworkDslV2 with(UnaryOperator<CuratorEvent> resultFilter, UnaryOperator<WatchedEvent> watcherFilter)
    {
        return new AsyncCuratorFrameworkV2Impl(reveal(client.with(resultFilter, watcherFilter)));
    }

    @Override
    public AsyncCuratorFrameworkDslV2 with(UnhandledErrorListener listener, UnaryOperator<CuratorEvent> resultFilter, UnaryOperator<WatchedEvent> watcherFilter)
    {
        return new AsyncCuratorFrameworkV2Impl(reveal(client.with(listener, resultFilter, watcherFilter)));
    }

    @Override
    public AsyncCuratorFrameworkDslV2 with(WatchMode mode, UnhandledErrorListener listener, UnaryOperator<CuratorEvent> resultFilter, UnaryOperator<WatchedEvent> watcherFilter)
    {
        return new AsyncCuratorFrameworkV2Impl(reveal(client.with(mode, listener, resultFilter, watcherFilter)));
    }

    @Override
    public AsyncWatchBuilder addWatch()
    {
        return new AsyncWatchBuilderImpl(client.getClient(), client.getFilters());
    }

    @Override
    public WatchableAsyncCuratorFramework watched()
    {
        return client.watched();
    }

    @Override
    public AsyncCreateBuilder create()
    {
        return client.create();
    }

    @Override
    public AsyncDeleteBuilder delete()
    {
        return client.delete();
    }

    @Override
    public AsyncSetDataBuilder setData()
    {
        return client.setData();
    }

    @Override
    public AsyncGetACLBuilder getACL()
    {
        return client.getACL();
    }

    @Override
    public AsyncSetACLBuilder setACL()
    {
        return client.setACL();
    }

    @Override
    public AsyncReconfigBuilder reconfig()
    {
        return client.reconfig();
    }

    @Override
    public AsyncMultiTransaction transaction()
    {
        return client.transaction();
    }

    @Override
    public AsyncTransactionOp transactionOp()
    {
        return client.transactionOp();
    }

    @Override
    public AsyncSyncBuilder sync()
    {
        return client.sync();
    }

    @Override
    public AsyncRemoveWatchesBuilder removeWatches()
    {
        return client.removeWatches();
    }

    @Override
    public AsyncExistsBuilder checkExists()
    {
        return client.checkExists();
    }

    @Override
    public AsyncGetDataBuilder getData()
    {
        return client.getData();
    }

    @Override
    public AsyncGetChildrenBuilder getChildren()
    {
        return client.getChildren();
    }

    @Override
    public AsyncGetConfigBuilder getConfig()
    {
        return client.getConfig();
    }
}
