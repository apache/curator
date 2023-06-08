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

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import org.apache.curator.RetryLoop;
import org.apache.curator.TimeTrace;
import org.apache.curator.framework.api.*;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.admin.ZooKeeperAdmin;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataTree;

public class ReconfigBuilderImpl
        implements ReconfigBuilder, BackgroundOperation<Void>, ErrorListenerReconfigBuilderMain {
    private final CuratorFrameworkImpl client;

    private Backgrounding backgrounding = new Backgrounding();
    private Stat responseStat;
    private long fromConfig = -1;
    private List<String> newMembers;
    private List<String> joining;
    private List<String> leaving;

    public ReconfigBuilderImpl(CuratorFrameworkImpl client) {
        this.client = client;
    }

    public ReconfigBuilderImpl(
            CuratorFrameworkImpl client,
            Backgrounding backgrounding,
            Stat responseStat,
            long fromConfig,
            List<String> newMembers,
            List<String> joining,
            List<String> leaving) {
        this.client = client;
        this.backgrounding = backgrounding;
        this.responseStat = responseStat;
        this.fromConfig = fromConfig;
        this.newMembers = newMembers;
        this.joining = joining;
        this.leaving = leaving;
    }

    public byte[] forEnsemble() throws Exception {
        if (backgrounding.inBackground()) {
            client.processBackgroundOperation(
                    new OperationAndData<>(
                            this, null, backgrounding.getCallback(), null, backgrounding.getContext(), null),
                    null);
            return new byte[0];
        } else {
            return ensembleInForeground();
        }
    }

    @Override
    public ErrorListenerReconfigBuilderMain inBackground() {
        backgrounding = new Backgrounding(true);
        return this;
    }

    @Override
    public ErrorListenerReconfigBuilderMain inBackground(Object context) {
        backgrounding = new Backgrounding(context);
        return this;
    }

    @Override
    public ErrorListenerReconfigBuilderMain inBackground(BackgroundCallback callback) {
        backgrounding = new Backgrounding(callback);
        return this;
    }

    @Override
    public ErrorListenerReconfigBuilderMain inBackground(BackgroundCallback callback, Object context) {
        backgrounding = new Backgrounding(callback, context);
        return this;
    }

    @Override
    public ErrorListenerReconfigBuilderMain inBackground(BackgroundCallback callback, Executor executor) {
        backgrounding = new Backgrounding(callback, executor);
        return this;
    }

    @Override
    public ErrorListenerReconfigBuilderMain inBackground(
            BackgroundCallback callback, Object context, Executor executor) {
        backgrounding = new Backgrounding(client, callback, context, executor);
        return this;
    }

    @Override
    public ReconfigBuilderMain withUnhandledErrorListener(UnhandledErrorListener listener) {
        backgrounding = new Backgrounding(backgrounding, listener);
        return this;
    }

    @Override
    public StatConfigureEnsembleable withNewMembers(String... server) {
        return withNewMembers((server != null) ? Arrays.asList(server) : null);
    }

    @Override
    public StatConfigureEnsembleable withNewMembers(List<String> servers) {
        newMembers = (servers != null && !servers.isEmpty()) ? ImmutableList.copyOf(servers) : null;
        return new StatConfigureEnsembleable() {
            @Override
            public Ensembleable<byte[]> fromConfig(long config) throws Exception {
                fromConfig = config;
                return this;
            }

            @Override
            public byte[] forEnsemble() throws Exception {
                return ReconfigBuilderImpl.this.forEnsemble();
            }

            @Override
            public ConfigureEnsembleable storingStatIn(Stat stat) {
                responseStat = stat;
                return this;
            }
        };
    }

    @Override
    public LeaveStatConfigEnsembleable joining(String... server) {
        return joining((server != null) ? Arrays.asList(server) : null);
    }

    @Override
    public LeaveStatConfigEnsembleable joining(List<String> servers) {
        joining = (servers != null && !servers.isEmpty()) ? ImmutableList.copyOf(servers) : null;

        return new LeaveStatConfigEnsembleable() {
            @Override
            public byte[] forEnsemble() throws Exception {
                return ReconfigBuilderImpl.this.forEnsemble();
            }

            @Override
            public ConfigureEnsembleable storingStatIn(Stat stat) {
                responseStat = stat;
                return this;
            }

            @Override
            public Ensembleable<byte[]> fromConfig(long config) throws Exception {
                fromConfig = config;
                return this;
            }

            @Override
            public JoinStatConfigEnsembleable leaving(String... server) {
                return ReconfigBuilderImpl.this.leaving(server);
            }

            @Override
            public JoinStatConfigEnsembleable leaving(List<String> servers) {
                return ReconfigBuilderImpl.this.leaving(servers);
            }
        };
    }

    @Override
    public JoinStatConfigEnsembleable leaving(String... server) {
        return leaving((server != null) ? Arrays.asList(server) : null);
    }

    @Override
    public JoinStatConfigEnsembleable leaving(List<String> servers) {
        leaving = (servers != null && !servers.isEmpty()) ? ImmutableList.copyOf(servers) : null;

        return new JoinStatConfigEnsembleable() {
            @Override
            public byte[] forEnsemble() throws Exception {
                return ReconfigBuilderImpl.this.forEnsemble();
            }

            @Override
            public ConfigureEnsembleable storingStatIn(Stat stat) {
                responseStat = stat;
                return this;
            }

            @Override
            public Ensembleable<byte[]> fromConfig(long config) throws Exception {
                fromConfig = config;
                return this;
            }

            @Override
            public LeaveStatConfigEnsembleable joining(String... server) {
                return joining((server != null) ? Arrays.asList(server) : null);
            }

            @Override
            public LeaveStatConfigEnsembleable joining(List<String> servers) {
                return ReconfigBuilderImpl.this.joining(servers);
            }
        };
    }

    @Override
    public CuratorEventType getBackgroundEventType() {
        return CuratorEventType.RECONFIG;
    }

    @Override
    public void performBackgroundOperation(final OperationAndData<Void> data) throws Exception {
        try {
            final TimeTrace trace = client.getZookeeperClient().startTracer("ReconfigBuilderImpl-Background");
            AsyncCallback.DataCallback callback = new AsyncCallback.DataCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, byte[] bytes, Stat stat) {
                    trace.commit();
                    if ((responseStat != null) && (stat != null)) {
                        DataTree.copyStat(stat, responseStat);
                    }
                    CuratorEvent event = new CuratorEventImpl(
                            client,
                            CuratorEventType.RECONFIG,
                            rc,
                            path,
                            null,
                            ctx,
                            stat,
                            bytes,
                            null,
                            null,
                            null,
                            null);
                    client.processBackgroundOperation(data, event);
                }
            };
            ((ZooKeeperAdmin) client.getZooKeeper())
                    .reconfigure(joining, leaving, newMembers, fromConfig, callback, backgrounding.getContext());
        } catch (Throwable e) {
            backgrounding.checkError(e, null);
        }
    }

    private byte[] ensembleInForeground() throws Exception {
        TimeTrace trace = client.getZookeeperClient().startTracer("ReconfigBuilderImpl-Foreground");
        byte[] responseData = RetryLoop.callWithRetry(client.getZookeeperClient(), new Callable<byte[]>() {
            @Override
            public byte[] call() throws Exception {
                return ((ZooKeeperAdmin) client.getZooKeeper())
                        .reconfigure(joining, leaving, newMembers, fromConfig, responseStat);
            }
        });
        trace.commit();
        return responseData;
    }
}
