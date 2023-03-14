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

package org.apache.curator.x.async.api;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.x.async.AsyncStage;
import org.apache.zookeeper.Watcher;
import java.util.Set;

/**
 * Builder for watcher removal
 */
public interface AsyncRemoveWatchesBuilder
{
    /**
     * @param watcher the watcher to remove
     * @return this
     */
    AsyncPathable<AsyncStage<Void>> removing(Watcher watcher);

    /**
     * @param watcher the watcher to remove
     * @return this
     */
    AsyncPathable<AsyncStage<Void>> removing(CuratorWatcher watcher);

    /**
     * Remove all watchers
     *
     * @return this
     */
    AsyncPathable<AsyncStage<Void>> removingAll();

    /**
     * @param watcher the watcher to remove
     * @param options watcher removal options
     * @return this
     */
    AsyncPathable<AsyncStage<Void>> removing(Watcher watcher, Set<RemoveWatcherOption> options);

    /**
     * @param watcher the watcher to remove
     * @param options watcher removal options
     * @return this
     */
    AsyncPathable<AsyncStage<Void>> removing(CuratorWatcher watcher, Set<RemoveWatcherOption> options);

    /**
     * Remove all watchers
     *
     * @param options watcher removal options
     * @return this
     */
    AsyncPathable<AsyncStage<Void>> removingAll(Set<RemoveWatcherOption> options);

    /**
     * Remove a watcher of a given type
     *
     * @param watcher the watcher to remove
     * @param watcherType watcher type
     * @param options watcher removal options
     * @return this
     */
    AsyncPathable<AsyncStage<Void>> removing(Watcher watcher, Watcher.WatcherType watcherType, Set<RemoveWatcherOption> options);

    /**
     * Remove a watcher of a given type
     *
     * @param watcher the watcher to remove
     * @param watcherType watcher type
     * @param options watcher removal options
     * @return this
     */
    AsyncPathable<AsyncStage<Void>> removing(CuratorWatcher watcher, Watcher.WatcherType watcherType, Set<RemoveWatcherOption> options);

    /**
     * Remove all watchers of a given type
     *
     * @param watcherType watcher type
     * @param options watcher removal options
     * @return this
     */
    AsyncPathable<AsyncStage<Void>> removingAll(Watcher.WatcherType watcherType, Set<RemoveWatcherOption> options);

    /**
     * Remove a watcher of a given type
     *
     * @param watcher the watcher to remove
     * @param watcherType watcher type
     * @return this
     */
    AsyncPathable<AsyncStage<Void>> removing(Watcher watcher, Watcher.WatcherType watcherType);

    /**
     * Remove a watcher of a given type
     *
     * @param watcher the watcher to remove
     * @param watcherType watcher type
     * @return this
     */
    AsyncPathable<AsyncStage<Void>> removing(CuratorWatcher watcher, Watcher.WatcherType watcherType);

    /**
     * Remove all watchers of a given type
     *
     * @param watcherType watcher type
     * @return this
     */
    AsyncPathable<AsyncStage<Void>> removingAll(Watcher.WatcherType watcherType);
}
