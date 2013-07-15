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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.zookeeper.ClientCnxn;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutionException;

class DispatchingWatcher implements Watcher, Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final LoadingCache<String, Set<NamespaceWatcher>> watchers = CacheBuilder
        .newBuilder()
        .build
        (
            new CacheLoader<String, Set<NamespaceWatcher>>()
            {
                @Override
                public Set<NamespaceWatcher> load(String key) throws Exception
                {
                    return Sets.newSetFromMap(Maps.<NamespaceWatcher, Boolean>newConcurrentMap());
                }
            }
        );

    @Override
    public void process(WatchedEvent watchedEvent)
    {
        String path = watchedEvent.getPath();
        Collection<NamespaceWatcher> watchersForPath;
        try
        {
            watchersForPath = (path != null) ? watchers.get(path) : Sets.<NamespaceWatcher>newHashSet();
        }
        catch ( ExecutionException e )
        {
            log.error("Unexpected error", e);
            throw new RuntimeException(e);  // should never happen
        }

        // We don't want to remove Watchers on None events (e.g. disconnected, expired etc).
        switch ( watchedEvent.getType() )
        {
            case None:
            {
                clearIfNeeded(watchedEvent.getState());
                break;
            }

            default:
            {
                watchers.invalidate(path);
                break;
            }
        }

        for ( NamespaceWatcher watcher : watchersForPath )
        {
            try
            {
                watcher.process(watchedEvent);
            }
            catch ( Exception e )
            {
                log.error("Error while calling watcher.", e);
            }
        }
    }

    /**
     * Registers a {@link NamespaceWatcher}.
     *
     * @param path    The registration path.
     * @param watcher The watcher.
     * @return The global watcher instance.
     */
    public Watcher addNamespaceWatcher(String path, NamespaceWatcher watcher)
    {
        try
        {
            watchers.get(path).add(watcher);
        }
        catch ( ExecutionException e )
        {
            log.error("Unexpected error", e);
            throw new RuntimeException(e);  // should never happen
        }
        return this;
    }

    @Override
    public void close()
    {
        watchers.invalidateAll();
    }

    /**
     * Clears all {@link Watcher} objects if needed.
     *
     * @param state The keeper state.
     */
    private void clearIfNeeded(Event.KeeperState state)
    {
        if ( ClientCnxn.getDisableAutoResetWatch() && (state != Event.KeeperState.SyncConnected) )
        {
            watchers.invalidateAll();
        }
    }
}
