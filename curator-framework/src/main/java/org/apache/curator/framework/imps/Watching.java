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
    private final Watcher watcher;
    private final CuratorWatcher curatorWatcher;
    private final boolean watched;

    Watching(boolean watched)
    {
        this.watcher = null;
        this.curatorWatcher = null;
        this.watched = watched;
    }

    Watching(Watcher watcher)
    {
        this.watcher = watcher;
        this.curatorWatcher = null;
        this.watched = false;
    }

    Watching(CuratorWatcher watcher)
    {
        this.watcher = null;
        this.curatorWatcher = watcher;
        this.watched = false;
    }

    Watching()
    {
        watcher = null;
        watched = false;
        curatorWatcher = null;
    }

    Watcher getWatcher(CuratorFrameworkImpl client, String unfixedPath)
    {
        NamespaceWatcher namespaceWatcher = null;
        if ( watcher != null )
        {
            namespaceWatcher = new NamespaceWatcher(client, this.watcher, unfixedPath);
        }
        else if ( curatorWatcher != null )
        {
            namespaceWatcher = new NamespaceWatcher(client, curatorWatcher, unfixedPath);
        }

        if ( namespaceWatcher != null )
        {
            if ( client.getWatcherRemovalManager() != null )
            {
                client.getWatcherRemovalManager().add(namespaceWatcher);
            }
        }

        return namespaceWatcher;
    }

    boolean isWatched()
    {
        return watched;
    }
}
