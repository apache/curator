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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Set;

public class WatcherRemovalManager
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorFrameworkImpl client;
    private final Set<NamespaceWatcher> entries = Sets.newConcurrentHashSet();

    WatcherRemovalManager(CuratorFrameworkImpl client)
    {
        this.client = client;
    }

    void add(NamespaceWatcher watcher)
    {
        watcher = Preconditions.checkNotNull(watcher, "watcher cannot be null");
        entries.add(watcher);
    }

    @VisibleForTesting
    Set<? extends Watcher> getEntries()
    {
        return Sets.newHashSet(entries);
    }

    void removeWatchers()
    {
        List<NamespaceWatcher> localEntries = Lists.newArrayList(entries);
        while ( localEntries.size() > 0 )
        {
            NamespaceWatcher watcher = localEntries.remove(0);
            if ( entries.remove(watcher) && !client.isZk34CompatibilityMode() )
            {
                try
                {
                    log.debug("Removing watcher for path: " + watcher.getUnfixedPath());
                    RemoveWatchesBuilderImpl builder = new RemoveWatchesBuilderImpl(client);
                    builder.internalRemoval(watcher, watcher.getUnfixedPath());
                }
                catch ( Exception e )
                {
                    log.error("Could not remove watcher for path: " + watcher.getUnfixedPath());
                }
            }
        }
    }

    void noteTriggeredWatcher(NamespaceWatcher watcher)
    {
        entries.remove(watcher);
    }
}
