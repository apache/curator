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

import org.apache.curator.RetryLoop;
import org.apache.curator.TimeTrace;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.BackgroundStatConfigurable;
import org.apache.curator.framework.api.Configurable;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.Ensembleable;
import org.apache.curator.framework.api.JoinBackgroundStatConfigurable;
import org.apache.curator.framework.api.LeaveBackgroundStatConfigurable;
import org.apache.curator.framework.api.ReconfigBuilder;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.data.Stat;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

public class ReconfigBuilderImpl implements ReconfigBuilder {

    private final CuratorFrameworkImpl client;

    public ReconfigBuilderImpl(CuratorFrameworkImpl client) {
        this.client = client;
    }

    private static class ReconfigBuilderBase implements BackgroundStatConfigurable<byte[]>, Ensembleable<byte[]>, BackgroundOperation<EnsembleServersAndConfig> {

        final CuratorFrameworkImpl client;
        final List<String> joiningServers = new LinkedList<String>();
        final List<String> leavingServers = new LinkedList<String>();
        final List<String> members = new LinkedList<String>();
        Backgrounding backgrounding;
        Stat stat;
        long config;

        private ReconfigBuilderBase(CuratorFrameworkImpl client) {
            this.client = client;
            backgrounding = new Backgrounding();
        }

        @Override
        public Configurable<byte[]> inBackground() {
            backgrounding = new Backgrounding();
            return this;
        }

        @Override
        public Configurable<byte[]> inBackground(Object context) {
            backgrounding = new Backgrounding(context);
            return this;
        }

        @Override
        public Configurable<byte[]> inBackground(BackgroundCallback callback) {
            backgrounding = new Backgrounding(callback);
            return this;
        }

        @Override
        public Configurable<byte[]> inBackground(BackgroundCallback callback, Object context) {
            backgrounding = new Backgrounding(callback, context);
            return this;
        }

        @Override
        public Configurable<byte[]> inBackground(BackgroundCallback callback, Executor executor) {
            backgrounding = new Backgrounding(callback, executor);
            return this;
        }

        @Override
        public Configurable<byte[]> inBackground(BackgroundCallback callback, Object context, Executor executor) {
            backgrounding = new Backgrounding(client, callback, context, executor);
            return this;
        }

        @Override
        public Ensembleable<byte[]> fromConfig(long config) throws Exception {
            this.config = config;
            return this;
        }

        @Override
        public Configurable<byte[]> storingStatIn(Stat stat) {
            this.stat = stat;
            return this;
        }

        @Override
        public byte[] forEnsemble() throws Exception {
            if (backgrounding.inBackground()) {
                client.processBackgroundOperation(new OperationAndData<EnsembleServersAndConfig>(this,
                        new EnsembleServersAndConfig(joiningServers, leavingServers, members, config),
                        backgrounding.getCallback(), null, backgrounding.getContext()), null);
                return new byte[0];
            } else {
                return ensembleInForeground();
            }
        }

        private byte[] ensembleInForeground() throws Exception {
            TimeTrace trace = client.getZookeeperClient().startTracer("ReconfigBuilderImpl-Foreground");
            byte[] responseData = RetryLoop.callWithRetry
                    (
                            client.getZookeeperClient(),
                            new Callable<byte[]>() {
                                @Override
                                public byte[] call() throws Exception {
                                    return client.getZooKeeper().reconfig(
                                            joiningServers.isEmpty() ? null : joiningServers,
                                            leavingServers.isEmpty() ? null : leavingServers,
                                            members.isEmpty() ? null : members,
                                            config,
                                            stat
                                    );
                                }
                            }
                    );
            trace.commit();
            return responseData;
        }

        @Override
        public void performBackgroundOperation(final OperationAndData<EnsembleServersAndConfig> operationAndData) throws Exception {
            final TimeTrace trace = client.getZookeeperClient().startTracer("ReconfigBuilderImpl-Background");
            AsyncCallback.DataCallback callback = new AsyncCallback.DataCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                    trace.commit();
                    CuratorEvent event = new CuratorEventImpl(client, CuratorEventType.RECONFIG, rc, path, null, ctx, stat, data, null, null, null);
                    client.processBackgroundOperation(operationAndData, event);
                }
            };
            client.getZooKeeper().reconfig(
                    operationAndData.getData().getJoiningServers(),
                    operationAndData.getData().getLeavingServers(),
                    operationAndData.getData().getMembers(),
                    operationAndData.getData().getConfig(),
                    callback,
                    operationAndData.getContext()
            );

        }
    }

    private static class JoinReconfigBuilder extends ReconfigBuilderBase implements JoinBackgroundStatConfigurable {

        private JoinReconfigBuilder(CuratorFrameworkImpl client) {
            super(client);
        }

        @Override
        public BackgroundStatConfigurable<byte[]> joining(String... servers) {
            joiningServers.addAll(Arrays.asList(servers));
            return this;
        }
    }

    private static class LeaveReconfigBuilder extends ReconfigBuilderBase implements LeaveBackgroundStatConfigurable {

        private LeaveReconfigBuilder(CuratorFrameworkImpl client) {
            super(client);
        }

        @Override
        public BackgroundStatConfigurable<byte[]> leaving(String... servers) {
            leavingServers.addAll(Arrays.asList(servers));
            return this;
        }
    }


    @Override
    public LeaveBackgroundStatConfigurable joining(String... servers) {
        LeaveReconfigBuilder builder = new LeaveReconfigBuilder(client);
        builder.joiningServers.addAll(Arrays.asList(servers));
        return builder;
    }

    @Override
    public JoinBackgroundStatConfigurable leaving(String... servers) {
        JoinReconfigBuilder builder = new JoinReconfigBuilder(client);
        builder.leavingServers.addAll(Arrays.asList(servers));
        return builder;
    }

    @Override
    public BackgroundStatConfigurable<byte[]> withMembers(String... servers) {
        ReconfigBuilderBase builder = new ReconfigBuilderBase(client);
        builder.members.addAll(Arrays.asList(servers));
        return builder;
    }
}
