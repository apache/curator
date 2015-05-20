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
package org.apache.curator.framework.imps;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.Watcher;

class Watching
{
    private final Watcher       watcher;
    private final boolean       watched;

    Watching(boolean watched)
    {
        this.watcher = null;
        this.watched = watched;
    }

    Watching(CuratorFrameworkImpl client, Watcher watcher)
    {
        this.watcher = (watcher != null) ? client.getNamespaceWatcherMap().getNamespaceWatcher(watcher) : null;
        this.watched = false;
    }

    Watching(CuratorFrameworkImpl client, CuratorWatcher watcher)
    {
        this.watcher = (watcher != null) ? client.getNamespaceWatcherMap().getNamespaceWatcher(watcher) : null;
        this.watched = false;
    }

    Watching()
    {
        watcher = null;
        watched = false;
    }

    Watcher getWatcher(CuratorFrameworkImpl client, String unfixedPath)
    {
        if ( (watcher != null) && (client.getWatcherRemovalManager() != null) )
        {
            return client.getWatcherRemovalManager().add(unfixedPath, watcher);
        }
        return watcher;
    }

    boolean isWatched()
    {
        return watched;
    }
}
