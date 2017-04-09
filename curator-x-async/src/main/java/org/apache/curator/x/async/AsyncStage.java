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
package org.apache.curator.x.async;

import org.apache.zookeeper.WatchedEvent;
import java.util.concurrent.CompletionStage;

/**
 * A {@link java.util.concurrent.CompletionStage} that is the result of most operations.
 */
public interface AsyncStage<T> extends CompletionStage<T>
{
    /**
     * <p>
     *     If the {@link org.apache.curator.x.async.api.WatchableAsyncCuratorFramework} facade is
     *     used (via {@link AsyncCuratorFramework#watched()}), this returns the completion
     *     stage used when the watcher is triggered
     * </p>
     *
     * <p>
     *     Also, applies to {@link org.apache.curator.x.async.modeled.ModeledCuratorFramework}
     *     when {@link org.apache.curator.x.async.modeled.ModeledCuratorFrameworkBuilder#watched(WatchMode)}
     *     or {@link org.apache.curator.x.async.modeled.ModeledCuratorFrameworkBuilder#watched(WatchMode, java.util.function.UnaryOperator)}
     *     is used.
     * </p>
     *
     * @return CompletionStage for the set watcher or <code>null</code>
     */
    CompletionStage<WatchedEvent> event();
}
