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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashSet;
import java.util.Set;

public class WatcherRemovalManager
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorFrameworkImpl client;
    private final NamespaceWatcherMap namespaceWatcherMap;
    private final Set<WrappedWatcher> entries = Sets.newHashSet();  // guarded by sync

    WatcherRemovalManager(CuratorFrameworkImpl client, NamespaceWatcherMap namespaceWatcherMap)
    {
        this.client = client;
        this.namespaceWatcherMap = namespaceWatcherMap;
    }

    synchronized Watcher add(String path, Watcher watcher)
    {
        path = Preconditions.checkNotNull(path, "path cannot be null");
        watcher = Preconditions.checkNotNull(watcher, "watcher cannot be null");

        WrappedWatcher wrappedWatcher = new WrappedWatcher(watcher, path);
        entries.add(wrappedWatcher);
        return wrappedWatcher;
    }

    @VisibleForTesting
    synchronized Set<? extends Watcher> getEntries()
    {
        return Sets.newHashSet(entries);
    }

    void removeWatchers()
    {
        HashSet<WrappedWatcher> localEntries;
        synchronized(this)
        {
            localEntries = Sets.newHashSet(entries);
        }
        for ( WrappedWatcher entry : localEntries )
        {
            try
            {
                log.debug("Removing watcher for path: " + entry.path);
                RemoveWatchesBuilderImpl builder = new RemoveWatchesBuilderImpl(client);
                namespaceWatcherMap.removeWatcher(entry.watcher);
                builder.internalRemoval(entry, entry.path);
            }
            catch ( Exception e )
            {
                log.error("Could not remove watcher for path: " + entry.path);
            }
        }
    }

    private synchronized void internalRemove(WrappedWatcher entry)
    {
        namespaceWatcherMap.removeWatcher(entry.watcher);
        entries.remove(entry);
    }

    private class WrappedWatcher implements Watcher
    {
        private final Watcher watcher;
        private final String path;

        WrappedWatcher(Watcher watcher, String path)
        {
            this.watcher = watcher;
            this.path = path;
        }

        @Override
        public void process(WatchedEvent event)
        {
            if ( event.getType() != Event.EventType.None )
            {
                internalRemove(this);
            }
            watcher.process(event);
        }

        @Override
        public boolean equals(Object o)
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            WrappedWatcher entry = (WrappedWatcher)o;

            //noinspection SimplifiableIfStatement
            if ( !watcher.equals(entry.watcher) )
            {
                return false;
            }
            return path.equals(entry.path);

        }

        @Override
        public int hashCode()
        {
            int result = watcher.hashCode();
            result = 31 * result + path.hashCode();
            return result;
        }
    }
}
