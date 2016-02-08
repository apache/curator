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

import com.google.common.base.Preconditions;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.utils.ThreadUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import java.io.Closeable;

class NamespaceWatcher implements Watcher, Closeable
{
    private volatile CuratorFrameworkImpl client;
    private volatile Watcher actualWatcher;
    private final String unfixedPath;
    private volatile CuratorWatcher curatorWatcher;

    NamespaceWatcher(CuratorFrameworkImpl client, Watcher actualWatcher, String unfixedPath)
    {
        this.client = client;
        this.actualWatcher = actualWatcher;
        this.unfixedPath = Preconditions.checkNotNull(unfixedPath, "unfixedPath cannot be null");
        this.curatorWatcher = null;
    }

    NamespaceWatcher(CuratorFrameworkImpl client, CuratorWatcher curatorWatcher, String unfixedPath)
    {
        this.client = client;
        this.actualWatcher = null;
        this.curatorWatcher = curatorWatcher;
        this.unfixedPath = Preconditions.checkNotNull(unfixedPath, "unfixedPath cannot be null");
    }

    String getUnfixedPath()
    {
        return unfixedPath;
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
            if ( (event.getType() != Event.EventType.None) && (client.getWatcherRemovalManager() != null) )
            {
                client.getWatcherRemovalManager().noteTriggeredWatcher(this);
            }

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
                    ThreadUtils.checkInterrupted(e);
                    client.logError("Watcher exception", e);
                }
            }
        }
    }

    // specialized equals()/hashCode() that makes this instance equal to the actual watcher

    @Override
    public boolean equals(Object o)
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null )
        {
            return false;
        }

        if ( getClass() == o.getClass() )
        {
            NamespaceWatcher watcher = (NamespaceWatcher)o;

            if ( !unfixedPath.equals(watcher.getUnfixedPath()) )
            {
                return false;
            }

            //noinspection SimplifiableIfStatement
            if ( actualWatcher != null ? !actualWatcher.equals(watcher.actualWatcher) : watcher.actualWatcher != null )
            {
                return false;
            }
            return curatorWatcher != null ? curatorWatcher.equals(watcher.curatorWatcher) : watcher.curatorWatcher == null;
        }

        if ( Watcher.class.isAssignableFrom(o.getClass()) )
        {
            return actualWatcher == o;
        }

        //noinspection SimplifiableIfStatement
        if ( CuratorWatcher.class.isAssignableFrom(o.getClass()) )
        {
            return curatorWatcher == o;
        }

        return false;
    }

    @Override
    public int hashCode()
    {
        int result = actualWatcher != null ? actualWatcher.hashCode() : 0;
        result = 31 * result + unfixedPath.hashCode();
        result = 31 * result + (curatorWatcher != null ? curatorWatcher.hashCode() : 0);
        return result;
    }
}
