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

import java.util.concurrent.CompletionStage;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * The exception type set for async watchers
 */
public abstract class AsyncEventException extends Exception {
    /**
     * Returns the error condition that temporarily triggered the watcher. NOTE: the watcher
     * will most likely still be set. Use {@link #reset()} to stage on the successful trigger
     *
     * @return state
     */
    public abstract Watcher.Event.KeeperState getKeeperState();

    /**
     * ZooKeeper temporarily triggers watchers when there is a connection event. However, the watcher
     * stays set for the original operation. Use this method to reset with a new completion stage
     * that will allow waiting for a successful trigger.
     *
     * @return new stage
     */
    public abstract CompletionStage<WatchedEvent> reset();
}
