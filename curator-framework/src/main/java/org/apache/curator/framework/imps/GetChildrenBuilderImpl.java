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

package org.apache.curator.framework.imps;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import org.apache.curator.RetryLoop;
import org.apache.curator.drivers.OperationTrace;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.BackgroundPathable;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.api.ErrorListenerPathable;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.api.Pathable;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.api.WatchPathable;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

public class GetChildrenBuilderImpl
        implements GetChildrenBuilder, BackgroundOperation<String>, ErrorListenerPathable<List<String>> {
    private final CuratorFrameworkImpl client;
    private Watching watching;
    private Backgrounding backgrounding;
    private Stat responseStat;

    GetChildrenBuilderImpl(CuratorFrameworkImpl client) {
        this.client = client;
        watching = new Watching(client);
        backgrounding = new Backgrounding();
        responseStat = null;
    }

    public GetChildrenBuilderImpl(
            CuratorFrameworkImpl client, Watcher watcher, Backgrounding backgrounding, Stat responseStat) {
        this.client = client;
        this.watching = new Watching(client, watcher);
        this.backgrounding = backgrounding;
        this.responseStat = responseStat;
    }

    @Override
    public WatchPathable<List<String>> storingStatIn(Stat stat) {
        responseStat = stat;
        return new WatchPathable<List<String>>() {
            @Override
            public List<String> forPath(String path) throws Exception {
                return GetChildrenBuilderImpl.this.forPath(path);
            }

            @Override
            public Pathable<List<String>> watched() {
                GetChildrenBuilderImpl.this.watched();
                return GetChildrenBuilderImpl.this;
            }

            @Override
            public Pathable<List<String>> usingWatcher(Watcher watcher) {
                GetChildrenBuilderImpl.this.usingWatcher(watcher);
                return GetChildrenBuilderImpl.this;
            }

            @Override
            public Pathable<List<String>> usingWatcher(CuratorWatcher watcher) {
                GetChildrenBuilderImpl.this.usingWatcher(watcher);
                return GetChildrenBuilderImpl.this;
            }
        };
    }

    @Override
    public ErrorListenerPathable<List<String>> inBackground(BackgroundCallback callback, Object context) {
        backgrounding = new Backgrounding(callback, context);
        return this;
    }

    @Override
    public ErrorListenerPathable<List<String>> inBackground(
            BackgroundCallback callback, Object context, Executor executor) {
        backgrounding = new Backgrounding(client, callback, context, executor);
        return this;
    }

    @Override
    public ErrorListenerPathable<List<String>> inBackground(BackgroundCallback callback) {
        backgrounding = new Backgrounding(callback);
        return this;
    }

    @Override
    public ErrorListenerPathable<List<String>> inBackground(BackgroundCallback callback, Executor executor) {
        backgrounding = new Backgrounding(client, callback, executor);
        return this;
    }

    @Override
    public ErrorListenerPathable<List<String>> inBackground() {
        backgrounding = new Backgrounding(true);
        return this;
    }

    @Override
    public ErrorListenerPathable<List<String>> inBackground(Object context) {
        backgrounding = new Backgrounding(context);
        return this;
    }

    @Override
    public Pathable<List<String>> withUnhandledErrorListener(UnhandledErrorListener listener) {
        backgrounding = new Backgrounding(backgrounding, listener);
        return this;
    }

    @Override
    public BackgroundPathable<List<String>> watched() {
        watching = new Watching(client, true);
        return this;
    }

    @Override
    public BackgroundPathable<List<String>> usingWatcher(Watcher watcher) {
        watching = new Watching(client, watcher);
        return this;
    }

    @Override
    public BackgroundPathable<List<String>> usingWatcher(CuratorWatcher watcher) {
        watching = new Watching(client, watcher);
        return this;
    }

    @Override
    public void performBackgroundOperation(final OperationAndData<String> operationAndData) throws Exception {
        try {
            final OperationTrace trace =
                    client.getZookeeperClient().startAdvancedTracer("GetChildrenBuilderImpl-Background");
            AsyncCallback.Children2Callback callback = new AsyncCallback.Children2Callback() {
                @Override
                public void processResult(int rc, String path, Object o, List<String> strings, Stat stat) {
                    watching.commitWatcher(rc, false);
                    trace.setReturnCode(rc)
                            .setPath(path)
                            .setWithWatcher(watching.hasWatcher())
                            .setStat(stat)
                            .commit();
                    if (strings == null) {
                        strings = Lists.newArrayList();
                    }
                    CuratorEventImpl event = new CuratorEventImpl(
                            client,
                            CuratorEventType.CHILDREN,
                            rc,
                            path,
                            null,
                            o,
                            stat,
                            null,
                            strings,
                            null,
                            null,
                            null);
                    client.processBackgroundOperation(operationAndData, event);
                }
            };
            if (watching.isWatched()) {
                client.getZooKeeper()
                        .getChildren(operationAndData.getData(), true, callback, backgrounding.getContext());
            } else {
                client.getZooKeeper()
                        .getChildren(
                                operationAndData.getData(),
                                watching.getWatcher(operationAndData.getData()),
                                callback,
                                backgrounding.getContext());
            }
        } catch (Throwable e) {
            backgrounding.checkError(e, watching);
        }
    }

    @Override
    public List<String> forPath(String path) throws Exception {
        client.getSchemaSet().getSchema(path).validateWatch(path, watching.isWatched() || watching.hasWatcher());

        path = client.fixForNamespace(path);

        List<String> children = null;
        if (backgrounding.inBackground()) {
            client.processBackgroundOperation(
                    new OperationAndData<String>(
                            this, path, backgrounding.getCallback(), null, backgrounding.getContext(), watching),
                    null);
        } else {
            children = pathInForeground(path);
        }
        return children;
    }

    private List<String> pathInForeground(final String path) throws Exception {
        OperationTrace trace = client.getZookeeperClient().startAdvancedTracer("GetChildrenBuilderImpl-Foreground");
        List<String> children = RetryLoop.callWithRetry(client.getZookeeperClient(), new Callable<List<String>>() {
            @Override
            public List<String> call() throws Exception {
                List<String> children;
                if (watching.isWatched()) {
                    children = client.getZooKeeper().getChildren(path, true, responseStat);
                } else {
                    children = client.getZooKeeper().getChildren(path, watching.getWatcher(path), responseStat);
                    watching.commitWatcher(KeeperException.NoNodeException.Code.OK.intValue(), false);
                }
                return children;
            }
        });
        trace.setPath(path)
                .setWithWatcher(watching.hasWatcher())
                .setStat(responseStat)
                .commit();
        return children;
    }
}
