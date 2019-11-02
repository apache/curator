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

import org.apache.curator.framework.api.AddWatchBuilder;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.api.WatchesBuilder;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.WatcherType;

public class WatchesBuilderImpl extends RemoveWatchesBuilderImpl implements WatchesBuilder
{
    public WatchesBuilderImpl(CuratorFrameworkImpl client)
    {
        super(client);
    }

    public WatchesBuilderImpl(CuratorFrameworkImpl client, Watcher watcher, CuratorWatcher curatorWatcher, WatcherType watcherType, boolean guaranteed, boolean local, boolean quietly, Backgrounding backgrounding)
    {
        super(client, watcher, curatorWatcher, watcherType, guaranteed, local, quietly, backgrounding);
    }

    @Override
    public AddWatchBuilder add()
    {
        return new AddWatchBuilderImpl(getClient());
    }
}