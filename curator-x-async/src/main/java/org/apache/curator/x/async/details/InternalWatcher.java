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

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import java.util.concurrent.CompletableFuture;

class InternalWatcher extends CompletableFuture<WatchedEvent> implements Watcher
{
    @Override
    public void process(WatchedEvent event)
    {
        switch ( event.getState() )
        {
            case ConnectedReadOnly:
            case SyncConnected:
            case SaslAuthenticated:
            {
                complete(event);
                break;
            }

            case Disconnected:
            {
                completeExceptionally(KeeperException.create(KeeperException.Code.CONNECTIONLOSS));
                break;
            }

            case AuthFailed:
            {
                completeExceptionally(KeeperException.create(KeeperException.Code.AUTHFAILED));
                break;
            }

            case Expired:
            {
                completeExceptionally(KeeperException.create(KeeperException.Code.SESSIONEXPIRED));
                break;
            }
        }
    }
}
