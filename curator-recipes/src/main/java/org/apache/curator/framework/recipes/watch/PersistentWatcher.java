/*
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
package org.apache.curator.framework.recipes.watch;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.listen.StandardListenerManager;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.ThreadUtils;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A managed persistent watcher. The watch will be managed such that it stays set through
 * connection lapses, etc.
 */
public class PersistentWatcher implements Closeable {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);
    private final StandardListenerManager<Watcher> listeners = StandardListenerManager.standard();
    private final StandardListenerManager<Runnable> resetListeners = StandardListenerManager.standard();
    private final ConnectionStateListener connectionStateListener = (client, newState) -> {
        if (newState.isConnected()) {
            reset();
        }
    };
    private final Watcher watcher = event -> listeners.forEach(w -> w.process(event));
    private final CuratorFramework client;
    private final String basePath;
    private final boolean recursive;

    private enum State {
        LATENT,
        STARTED,
        CLOSED
    }

    /**
     * @param client client
     * @param basePath path to set the watch on
     * @param recursive ZooKeeper persistent watches can optionally be recursive
     */
    public PersistentWatcher(CuratorFramework client, String basePath, boolean recursive) {
        this.client = Objects.requireNonNull(client, "client cannot be null");
        this.basePath = Objects.requireNonNull(basePath, "basePath cannot be null");
        this.recursive = recursive;
    }

    /**
     * Start watching
     */
    public void start() {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");
        client.getConnectionStateListenable().addListener(connectionStateListener);
        reset();
    }

    /**
     * Remove the watcher
     */
    @Override
    public void close() {
        if (state.compareAndSet(State.STARTED, State.CLOSED)) {
            listeners.clear();
            client.getConnectionStateListenable().removeListener(connectionStateListener);
            try {
                client.watchers().remove(watcher).guaranteed().inBackground().forPath(basePath);
            } catch (Exception e) {
                ThreadUtils.checkInterrupted(e);
                log.debug(String.format("Could not remove watcher for path: %s", basePath), e);
            }
        }
    }

    /**
     * Container for setting listeners
     *
     * @return listener container
     */
    public Listenable<Watcher> getListenable() {
        return listeners;
    }

    /**
     * Listeners are called when the persistent watcher has been successfully registered
     * or re-registered after a connection disruption
     *
     * @return listener container
     */
    public Listenable<Runnable> getResetListenable() {
        return resetListeners;
    }

    private void reset() {
        if (state.get() != State.STARTED) {
            return;
        }

        try {
            BackgroundCallback callback = (__, event) -> {
                if (event.getResultCode() == KeeperException.Code.OK.intValue()) {
                    resetListeners.forEach(Runnable::run);
                } else {
                    reset();
                }
            };
            client.watchers()
                    .add()
                    .withMode(recursive ? AddWatchMode.PERSISTENT_RECURSIVE : AddWatchMode.PERSISTENT)
                    .inBackground(callback)
                    .usingWatcher(watcher)
                    .forPath(basePath);
        } catch (Exception e) {
            log.error("Could not reset persistent watch at path: " + basePath, e);
        }
    }
}
