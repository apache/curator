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

package org.apache.curator.x.async.details;

import java.util.List;
import java.util.Objects;
import org.apache.curator.framework.api.ACLPathAndBytesable;
import org.apache.curator.framework.api.PathAndBytesable;
import org.apache.curator.framework.api.VersionPathAndBytesable;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.TransactionCreateBuilder2;
import org.apache.curator.framework.api.transaction.TransactionSetDataBuilder;
import org.apache.curator.framework.imps.CuratorFrameworkImpl;
import org.apache.curator.x.async.api.AsyncPathAndBytesable;
import org.apache.curator.x.async.api.AsyncPathable;
import org.apache.curator.x.async.api.AsyncTransactionCheckBuilder;
import org.apache.curator.x.async.api.AsyncTransactionCreateBuilder;
import org.apache.curator.x.async.api.AsyncTransactionDeleteBuilder;
import org.apache.curator.x.async.api.AsyncTransactionOp;
import org.apache.curator.x.async.api.AsyncTransactionSetDataBuilder;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;

class AsyncTransactionOpImpl implements AsyncTransactionOp {
    private final CuratorFrameworkImpl client;

    AsyncTransactionOpImpl(CuratorFrameworkImpl client) {
        this.client = client;
    }

    @Override
    public AsyncTransactionCreateBuilder create() {
        return new AsyncTransactionCreateBuilder() {
            private List<ACL> aclList = null;
            private CreateMode createMode = CreateMode.PERSISTENT;
            private boolean compressed = client.compressionEnabled();
            private long ttl = -1;

            @Override
            public AsyncPathAndBytesable<CuratorOp> withMode(CreateMode createMode) {
                this.createMode = Objects.requireNonNull(createMode, "createMode cannot be null");
                return this;
            }

            @Override
            public AsyncPathAndBytesable<CuratorOp> withACL(List<ACL> aclList) {
                this.aclList = aclList;
                return this;
            }

            @Override
            public AsyncPathAndBytesable<CuratorOp> compressed() {
                compressed = true;
                return this;
            }

            @Override
            public AsyncPathAndBytesable<CuratorOp> uncompressed() {
                compressed = false;
                return this;
            }

            @Override
            public AsyncPathAndBytesable<CuratorOp> withTtl(long ttl) {
                this.ttl = ttl;
                return this;
            }

            @Override
            public AsyncPathAndBytesable<CuratorOp> withOptions(
                    CreateMode createMode, List<ACL> aclList, boolean compressed) {
                return withOptions(createMode, aclList, compressed, ttl);
            }

            @Override
            public AsyncPathAndBytesable<CuratorOp> withOptions(
                    CreateMode createMode, List<ACL> aclList, boolean compressed, long ttl) {
                this.createMode = Objects.requireNonNull(createMode, "createMode cannot be null");
                this.aclList = aclList;
                this.compressed = compressed;
                this.ttl = ttl;
                return this;
            }

            @Override
            public CuratorOp forPath(String path, byte[] data) {
                return internalForPath(path, data, true);
            }

            @Override
            public CuratorOp forPath(String path) {
                return internalForPath(path, null, false);
            }

            private CuratorOp internalForPath(String path, byte[] data, boolean useData) {
                TransactionCreateBuilder2<CuratorOp> builder1 = (ttl > 0)
                        ? client.transactionOp().create().withTtl(ttl)
                        : client.transactionOp().create();
                ACLPathAndBytesable<CuratorOp> builder2 = compressed
                        ? builder1.compressed().withMode(createMode)
                        : builder1.uncompressed().withMode(createMode);
                PathAndBytesable<CuratorOp> builder3 = builder2.withACL(aclList);
                try {
                    return useData ? builder3.forPath(path, data) : builder3.forPath(path);
                } catch (Exception e) {
                    throw new RuntimeException(e); // should never happen
                }
            }
        };
    }

    @Override
    public AsyncTransactionDeleteBuilder delete() {
        return new AsyncTransactionDeleteBuilder() {
            private int version = -1;

            @Override
            public AsyncPathable<CuratorOp> withVersion(int version) {
                this.version = version;
                return this;
            }

            @Override
            public CuratorOp forPath(String path) {
                try {
                    return client.transactionOp().delete().withVersion(version).forPath(path);
                } catch (Exception e) {
                    throw new RuntimeException(e); // should never happen
                }
            }
        };
    }

    @Override
    public AsyncTransactionSetDataBuilder setData() {
        return new AsyncTransactionSetDataBuilder() {
            private int version = -1;
            private boolean compressed = client.compressionEnabled();

            @Override
            public AsyncPathAndBytesable<CuratorOp> withVersion(int version) {
                this.version = version;
                return this;
            }

            @Override
            public AsyncPathAndBytesable<CuratorOp> compressed() {
                compressed = true;
                return this;
            }

            @Override
            public AsyncPathAndBytesable<CuratorOp> uncompressed() {
                compressed = false;
                return this;
            }

            @Override
            public AsyncPathAndBytesable<CuratorOp> withVersionCompressed(int version) {
                this.version = version;
                compressed = true;
                return this;
            }

            @Override
            public AsyncPathAndBytesable<CuratorOp> withVersionUncompressed(int version) {
                this.version = version;
                compressed = false;
                return this;
            }

            @Override
            public CuratorOp forPath(String path, byte[] data) {
                return internalForPath(path, data, true);
            }

            @Override
            public CuratorOp forPath(String path) {
                return internalForPath(path, null, false);
            }

            private CuratorOp internalForPath(String path, byte[] data, boolean useData) {
                TransactionSetDataBuilder<CuratorOp> builder1 =
                        client.transactionOp().setData();
                VersionPathAndBytesable<CuratorOp> builder2 =
                        compressed ? builder1.compressed() : builder1.uncompressed();
                PathAndBytesable<CuratorOp> builder3 = builder2.withVersion(version);
                try {
                    return useData ? builder3.forPath(path, data) : builder3.forPath(path);
                } catch (Exception e) {
                    throw new RuntimeException(e); // should never happen
                }
            }
        };
    }

    @Override
    public AsyncTransactionCheckBuilder check() {
        return new AsyncTransactionCheckBuilder() {
            private int version = -1;

            @Override
            public AsyncPathable<CuratorOp> withVersion(int version) {
                this.version = version;
                return this;
            }

            @Override
            public CuratorOp forPath(String path) {
                try {
                    return client.transactionOp().check().withVersion(version).forPath(path);
                } catch (Exception e) {
                    throw new RuntimeException(e); // should never happen
                }
            }
        };
    }
}
