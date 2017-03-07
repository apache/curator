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
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.x.async.AsyncStage;
import org.apache.zookeeper.WatchedEvent;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.UnaryOperator;

class InternalCallback<T> extends CompletableFuture<T> implements BackgroundCallback, AsyncStage<T>
{
    private final BackgroundProc<T> resultFunction;
    private final InternalWatcher watcher;
    private final UnaryOperator<CuratorEvent> resultFilter;

    InternalCallback(BackgroundProc<T> resultFunction, InternalWatcher watcher, UnaryOperator<CuratorEvent> resultFilter)
    {
        this.resultFunction = resultFunction;
        this.watcher = watcher;
        this.resultFilter = resultFilter;
    }

    @Override
    public CompletionStage<WatchedEvent> event()
    {
        return (watcher != null) ? watcher.getFuture() : null;
    }

    @Override
    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
    {
        event = (resultFilter != null) ? resultFilter.apply(event) : event;
        resultFunction.apply(event, this);
    }
}
