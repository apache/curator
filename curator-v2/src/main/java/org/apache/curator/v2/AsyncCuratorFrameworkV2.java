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
package org.apache.curator.v2;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.WatchMode;
import org.apache.curator.x.async.api.AsyncCuratorFrameworkDslV2;
import org.apache.curator.x.async.details.AsyncCuratorFrameworkV2Impl;
import org.apache.zookeeper.WatchedEvent;
import java.util.function.UnaryOperator;

public interface AsyncCuratorFrameworkV2 extends AsyncCuratorFramework, AsyncCuratorFrameworkDslV2
{
    /**
     * Wrap a CuratorFramework instance to gain access to newer ZooKeeper features
     *
     * @param client client instance
     * @return wrapped client
     */
    static AsyncCuratorFrameworkV2 wrap(CuratorFramework client)
    {
        return new AsyncCuratorFrameworkV2Impl(AsyncCuratorFramework.wrap(client));
    }

    /**
     * Wrap a AsyncCuratorFramework instance to gain access to newer ZooKeeper features
     *
     * @param client client instance
     * @return wrapped client
     */
    static AsyncCuratorFrameworkV2 wrap(AsyncCuratorFramework client)
    {
        return new AsyncCuratorFrameworkV2Impl(client);
    }

    @Override
    AsyncCuratorFrameworkDslV2 with(WatchMode mode);

    @Override
    AsyncCuratorFrameworkDslV2 with(UnhandledErrorListener listener);

    @Override
    AsyncCuratorFrameworkDslV2 with(UnaryOperator<CuratorEvent> resultFilter, UnaryOperator<WatchedEvent> watcherFilter);

    @Override
    AsyncCuratorFrameworkDslV2 with(UnhandledErrorListener listener, UnaryOperator<CuratorEvent> resultFilter, UnaryOperator<WatchedEvent> watcherFilter);

    @Override
    AsyncCuratorFrameworkDslV2 with(WatchMode mode, UnhandledErrorListener listener, UnaryOperator<CuratorEvent> resultFilter, UnaryOperator<WatchedEvent> watcherFilter);
}
