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
import org.apache.curator.framework.api.GetConfigBuilder;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

public class GetConfigBuilderImpl implements GetConfigBuilder {

    private final CuratorFrameworkImpl client;
    private boolean watched;
    private Watcher watcher;

    public GetConfigBuilderImpl(CuratorFrameworkImpl client) {
        this.client = client;
    }

    @Override
    public Void usingDataCallback(AsyncCallback.DataCallback callback, Object ctx) {
        try {
            if (watcher != null) {
                client.getZooKeeper().getConfig(watcher, callback, ctx);
            } else {
                client.getZooKeeper().getConfig(watched, callback, ctx);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    @Override
    public byte[] storingStatIn(Stat stat) {
        try {
            if (watcher != null) {
               return client.getZooKeeper().getConfig(watcher, stat);
            } else {
                return client.getZooKeeper().getConfig(watched, stat);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public GetConfigBuilder watched() {
        this.watched = true;
        return this;
    }

    @Override
    public GetConfigBuilder usingWatcher(Watcher watcher) {
        this.watcher = watcher;
        return null;
    }

    @Override
    public GetConfigBuilder usingWatcher(final CuratorWatcher watcher) {
        throw new UnsupportedOperationException("GetConfigBuilder doesn't support CuratorWatcher, please use Watcher instead.");
    }
}
