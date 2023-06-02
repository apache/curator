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

package org.apache.curator.x.async.details;

import com.google.common.base.Preconditions;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.UnaryOperator;
import org.apache.curator.x.async.AsyncEventException;
import org.apache.curator.x.async.WatchMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

class InternalWatcher implements Watcher {
    private final WatchMode watchMode;
    private final UnaryOperator<WatchedEvent> watcherFilter;
    private volatile CompletableFuture<WatchedEvent> future = new CompletableFuture<>();

    InternalWatcher(WatchMode watchMode, UnaryOperator<WatchedEvent> watcherFilter) {
        this.watchMode = watchMode;
        this.watcherFilter = watcherFilter;
    }

    CompletableFuture<WatchedEvent> getFuture() {
        return future;
    }

    @Override
    public void process(WatchedEvent event) {
        final WatchedEvent localEvent = (watcherFilter != null) ? watcherFilter.apply(event) : event;
        switch (localEvent.getState()) {
            default: {
                if ((watchMode != WatchMode.stateChangeOnly) && (localEvent.getType() != Event.EventType.None)) {
                    if (!future.complete(localEvent)) {
                        future.obtrudeValue(localEvent);
                    }
                }
                break;
            }

            case Disconnected:
            case AuthFailed:
            case Expired: {
                if (watchMode != WatchMode.successOnly) {
                    AsyncEventException exception = new AsyncEventException() {
                        private final AtomicBoolean isReset = new AtomicBoolean(false);

                        @Override
                        public Event.KeeperState getKeeperState() {
                            return localEvent.getState();
                        }

                        @Override
                        public CompletionStage<WatchedEvent> reset() {
                            Preconditions.checkState(isReset.compareAndSet(false, true), "Already reset");
                            future = new CompletableFuture<>();
                            return future;
                        }
                    };
                    future.completeExceptionally(exception);
                }
                break;
            }
        }
    }
}
