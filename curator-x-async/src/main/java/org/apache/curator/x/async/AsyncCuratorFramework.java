/*
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

import java.util.function.UnaryOperator;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.x.async.api.AsyncCuratorFrameworkDsl;
import org.apache.curator.x.async.details.AsyncCuratorFrameworkImpl;
import org.apache.zookeeper.WatchedEvent;

/**
 * Zookeeper framework-style client that returns composable async operations
 * that implement {@link java.util.concurrent.CompletionStage}
 */
public interface AsyncCuratorFramework extends AsyncCuratorFrameworkDsl {
    /**
     * Takes an old-style Curator instance and returns a new async instance that
     * wraps it. Note: the instance must have been created through a chain that
     * leads back to {@link org.apache.curator.framework.CuratorFrameworkFactory}. i.e.
     * you can have derived instances such as {@link org.apache.curator.framework.WatcherRemoveCuratorFramework}
     * etc. but the original client must have been created by the Factory.
     *
     * @param client instance to wrap
     * @return wrapped instance
     */
    static AsyncCuratorFramework wrap(CuratorFramework client) {
        return new AsyncCuratorFrameworkImpl(client);
    }

    /**
     * Returns the client that was originally passed to {@link #wrap(org.apache.curator.framework.CuratorFramework)}
     *
     * @return original client
     */
    CuratorFramework unwrap();

    /**
     * Returns a facade that changes how watchers are set when {@link #watched()} is called
     *
     * @param mode watch mode to use for subsequent calls to {@link #watched()}
     * @return facade
     */
    AsyncCuratorFrameworkDsl with(WatchMode mode);

    /**
     * Returns a facade that adds the given UnhandledErrorListener to all background operations
     *
     * @param listener lister to use
     * @return facade
     */
    AsyncCuratorFrameworkDsl with(UnhandledErrorListener listener);

    /**
     * Returns a facade that adds the the given filters to all background operations and watchers.
     * <code>resultFilter</code> will get called for every background callback. <code>watcherFilter</code>
     * will get called for every watcher. The filters can return new versions or unchanged versions
     * of the arguments.
     *
     * @param resultFilter filter to use or <code>null</code>
     * @param watcherFilter filter to use or <code>null</code>
     * @return facade
     */
    AsyncCuratorFrameworkDsl with(UnaryOperator<CuratorEvent> resultFilter, UnaryOperator<WatchedEvent> watcherFilter);

    /**
     * Set any combination of listener or filters
     *
     * @param listener lister to use or <code>null</code>
     * @param resultFilter filter to use or <code>null</code>
     * @param watcherFilter filter to use or <code>null</code>
     * @see #with(java.util.function.UnaryOperator, java.util.function.UnaryOperator)
     * @see #with(org.apache.curator.framework.api.UnhandledErrorListener)
     * @return facade
     */
    AsyncCuratorFrameworkDsl with(
            UnhandledErrorListener listener,
            UnaryOperator<CuratorEvent> resultFilter,
            UnaryOperator<WatchedEvent> watcherFilter);

    /**
     * Set any combination of listener, filters or watch mode
     *
     * @param mode watch mode to use for subsequent calls to {@link #watched()} (cannot be <code>null</code>)
     * @param listener lister to use or <code>null</code>
     * @param resultFilter filter to use or <code>null</code>
     * @param watcherFilter filter to use or <code>null</code>
     * @see #with(WatchMode)
     * @see #with(java.util.function.UnaryOperator, java.util.function.UnaryOperator)
     * @see #with(org.apache.curator.framework.api.UnhandledErrorListener)
     * @return facade
     */
    AsyncCuratorFrameworkDsl with(
            WatchMode mode,
            UnhandledErrorListener listener,
            UnaryOperator<CuratorEvent> resultFilter,
            UnaryOperator<WatchedEvent> watcherFilter);
}
