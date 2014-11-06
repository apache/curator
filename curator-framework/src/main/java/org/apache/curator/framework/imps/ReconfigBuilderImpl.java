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

import org.apache.curator.framework.api.AsyncReconfigurable;
import org.apache.curator.framework.api.IncrementalReconfigBuilder;
import org.apache.curator.framework.api.NonIncrementalReconfigBuilder;
import org.apache.curator.framework.api.ReconfigBuilder;
import org.apache.curator.framework.api.SyncReconfigurable;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.data.Stat;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class ReconfigBuilderImpl implements ReconfigBuilder {

    private final CuratorFrameworkImpl client;

    private static class IncrementalReconfigBuilderImpl<T> implements IncrementalReconfigBuilder<T> {

        private final CuratorFrameworkImpl client;
        private List<String> joiningServers = new LinkedList<String>();
        private List<String> leavingServers = new LinkedList<String>();

        private IncrementalReconfigBuilderImpl(CuratorFrameworkImpl client) {
            this.client = client;
        }

        @Override
        public IncrementalReconfigBuilderImpl<T> join(String server) {
            joiningServers.add(server);
            return this;
        }

        @Override
        public IncrementalReconfigBuilder<T> join(Collection<String> servers) {
            joiningServers.addAll(servers);
            return this;
        }

        @Override
        public IncrementalReconfigBuilderImpl<T> leave(String server) {
            leavingServers.add(server);
            return this;
        }

        @Override
        public IncrementalReconfigBuilder<T> leave(Collection<String> servers) {
            leavingServers.addAll(servers);
            return this;
        }

        @Override
        public SyncReconfigurable storingStatIn(final Stat stat) {
            return new SyncReconfigurable() {
                @Override
                public byte[] fromConfig(long config) throws Exception {
                    return client
                            .getZooKeeper()
                            .reconfig(joiningServers.isEmpty() ? null : joiningServers,
                                      leavingServers.isEmpty() ? null : leavingServers,
                                      null,
                                      config, stat);
                }
            };
        }

        @Override
        public AsyncReconfigurable usingDataCallback(final DataCallback callback, final Object ctx) {
            return new AsyncReconfigurable() {
                @Override
                public void fromConfig(long config) throws Exception {
                     client.getZooKeeper()
                           .reconfig(joiningServers.isEmpty() ? null : joiningServers,
                                     leavingServers.isEmpty() ? null : leavingServers,
                                     null,
                                     config, callback, ctx);
                }
            };
        }
    }

    private static class NonIncrementalReconfigBuilderImpl<T> implements NonIncrementalReconfigBuilder<T> {

        private final CuratorFrameworkImpl client;
        private List<String> newMembers = new LinkedList<String>();

        private NonIncrementalReconfigBuilderImpl(CuratorFrameworkImpl client) {
            this.client = client;
        }

        private NonIncrementalReconfigBuilderImpl(CuratorFrameworkImpl client, List<String> newMembers) {
            this.client = client;
            this.newMembers = newMembers;
        }

        @Override
        public NonIncrementalReconfigBuilder<T> withMember(String server) {
            newMembers.add(server);
            return this;
        }

        @Override
        public NonIncrementalReconfigBuilder<T> withMembers(Collection servers) {
            newMembers.addAll(servers);
            return this;
        }

        @Override
        public SyncReconfigurable storingStatIn(final Stat stat) {
            return new SyncReconfigurable() {
                @Override
                public byte[] fromConfig(long config) throws Exception {
                    return client.getZooKeeper().reconfig(null, null, newMembers, config, stat);
                }
            };
        }

        @Override
        public AsyncReconfigurable usingDataCallback(final DataCallback callback, final Object ctx) {
            return new AsyncReconfigurable() {
                @Override
                public void fromConfig(long config) throws Exception {
                    client.getZooKeeper().reconfig(null, null, newMembers, config, callback, ctx);
                }
            };
        }
    }


    public ReconfigBuilderImpl(CuratorFrameworkImpl client) {
        this.client = client;
    }

    @Override
    public IncrementalReconfigBuilder<byte[]> join(String server) {
        return new IncrementalReconfigBuilderImpl(client).join(server);
    }

    @Override
    public IncrementalReconfigBuilder<byte[]> join(Collection<String> servers) {
        return new IncrementalReconfigBuilderImpl(client).join(servers);
    }

    @Override
    public IncrementalReconfigBuilder<byte[]> leave(String server) {
        return new IncrementalReconfigBuilderImpl(client).leave(server);
    }

    @Override
    public IncrementalReconfigBuilder<byte[]> leave(Collection<String> servers) {
        return new IncrementalReconfigBuilderImpl(client).leave(servers);
    }

    @Override
    public NonIncrementalReconfigBuilder<byte[]> withMember(String server) {
        return new NonIncrementalReconfigBuilderImpl(client).withMember(server);
    }

    @Override
    public NonIncrementalReconfigBuilder<byte[]> withMembers(Collection<String> servers) {
        return new NonIncrementalReconfigBuilderImpl<byte[]>(client).withMembers(servers);
    }
}
