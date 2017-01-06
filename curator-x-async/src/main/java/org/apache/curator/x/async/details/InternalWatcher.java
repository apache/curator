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

import org.apache.curator.x.async.AsyncEventException;
import org.apache.curator.x.async.WatchMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

class InternalWatcher implements Watcher
{
    private final WatchMode watchMode;
    private volatile CompletableFuture<WatchedEvent> future = new CompletableFuture<>();

    InternalWatcher(WatchMode watchMode)
    {
        this.watchMode = watchMode;
    }

    CompletableFuture<WatchedEvent> getFuture()
    {
        return future;
    }

    @Override
    public void process(WatchedEvent event)
    {
        switch ( event.getState() )
        {
            default:
            {
                if ( (watchMode != WatchMode.stateChangeOnly) && (event.getType() != Event.EventType.None) )
                {
                    if ( !future.complete(event) )
                    {
                        future.obtrudeValue(event);
                    }
                }
                break;
            }

            case Disconnected:
            case AuthFailed:
            case Expired:
            {
                if ( watchMode != WatchMode.successOnly )
                {
                    AsyncEventException exception = new AsyncEventException()
                    {
                        @Override
                        public Event.KeeperState getKeeperState()
                        {
                            return event.getState();
                        }

                        @Override
                        public CompletionStage<WatchedEvent> reset()
                        {
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
