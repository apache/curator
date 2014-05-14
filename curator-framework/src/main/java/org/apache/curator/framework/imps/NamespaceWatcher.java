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
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import java.io.Closeable;

class NamespaceWatcher implements Watcher, Closeable
{
    private volatile CuratorFrameworkImpl client;
    private volatile Watcher actualWatcher;
    private volatile CuratorWatcher curatorWatcher;

    NamespaceWatcher(CuratorFrameworkImpl client, Watcher actualWatcher)
    {
        this.client = client;
        this.actualWatcher = actualWatcher;
        this.curatorWatcher = null;
    }

    NamespaceWatcher(CuratorFrameworkImpl client, CuratorWatcher curatorWatcher)
    {
        this.client = client;
        this.actualWatcher = null;
        this.curatorWatcher = curatorWatcher;
    }

    @Override
    public void close()
    {
        client = null;
        actualWatcher = null;
        curatorWatcher = null;
    }

    @Override
    public void process(WatchedEvent event)
    {
        if ( client != null )
        {
            if ( actualWatcher != null )
            {
                actualWatcher.process(new NamespaceWatchedEvent(client, event));
            }
            else if ( curatorWatcher != null )
            {
                try
                {
                    curatorWatcher.process(new NamespaceWatchedEvent(client, event));
                }
                catch ( Exception e )
                {
                    client.logError("Watcher exception", e);
                }
            }
        }
    }
}
