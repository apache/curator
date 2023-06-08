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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.curator.RetryLoop;
import org.apache.curator.drivers.OperationTrace;
import org.apache.curator.framework.api.ACLBackgroundPathAndBytesable;
import org.apache.curator.framework.api.ACLCreateModeBackgroundPathAndBytesable;
import org.apache.curator.framework.api.ACLCreateModePathAndBytesable;
import org.apache.curator.framework.api.ACLCreateModeStatBackgroundPathAndBytesable;
import org.apache.curator.framework.api.ACLPathAndBytesable;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.BackgroundPathAndBytesable;
import org.apache.curator.framework.api.CreateBackgroundModeACLable;
import org.apache.curator.framework.api.CreateBackgroundModeStatACLable;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.CreateBuilder2;
import org.apache.curator.framework.api.CreateBuilderMain;
import org.apache.curator.framework.api.CreateProtectACLCreateModePathAndBytesable;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.ErrorListenerPathAndBytesable;
import org.apache.curator.framework.api.PathAndBytesable;
import org.apache.curator.framework.api.ProtectACLCreateModePathAndBytesable;
import org.apache.curator.framework.api.ProtectACLCreateModeStatPathAndBytesable;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.api.transaction.OperationType;
import org.apache.curator.framework.api.transaction.TransactionCreateBuilder;
import org.apache.curator.framework.api.transaction.TransactionCreateBuilder2;
import org.apache.curator.utils.InternalACLProvider;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateBuilderImpl
        implements CreateBuilder,
                CreateBuilder2,
                BackgroundOperation<PathAndBytes>,
                ErrorListenerPathAndBytesable<String> {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorFrameworkImpl client;
    private final ProtectedMode protectedMode = new ProtectedMode();
    private CreateMode createMode;
    private Backgrounding backgrounding;
    private boolean createParentsIfNeeded;
    private boolean createParentsAsContainers;
    private boolean compress;
    private boolean setDataIfExists;
    private int setDataIfExistsVersion = -1;
    private boolean idempotent = false;
    private ACLing acling;
    private Stat storingStat;
    private long ttl;

    @VisibleForTesting
    boolean failNextCreateForTesting = false;

    @VisibleForTesting
    boolean failBeforeNextCreateForTesting = false;

    @VisibleForTesting
    boolean failNextIdempotentCheckForTesting = false;

    CreateBuilderImpl(CuratorFrameworkImpl client) {
        this.client = client;
        createMode = CreateMode.PERSISTENT;
        backgrounding = new Backgrounding();
        acling = new ACLing(client.getAclProvider());
        createParentsIfNeeded = false;
        createParentsAsContainers = false;
        compress = false;
        setDataIfExists = false;
        storingStat = null;
        ttl = -1;
    }

    public CreateBuilderImpl(
            CuratorFrameworkImpl client,
            CreateMode createMode,
            Backgrounding backgrounding,
            boolean createParentsIfNeeded,
            boolean createParentsAsContainers,
            boolean doProtected,
            boolean compress,
            boolean setDataIfExists,
            List<ACL> aclList,
            Stat storingStat,
            long ttl) {
        this.client = client;
        this.createMode = createMode;
        this.backgrounding = backgrounding;
        this.createParentsIfNeeded = createParentsIfNeeded;
        this.createParentsAsContainers = createParentsAsContainers;
        this.compress = compress;
        this.setDataIfExists = setDataIfExists;
        this.acling = new ACLing(client.getAclProvider(), aclList);
        this.storingStat = storingStat;
        this.ttl = ttl;
        if (doProtected) {
            protectedMode.setProtectedMode();
        }
    }

    public void setSetDataIfExistsVersion(int version) {
        this.setDataIfExistsVersion = version;
    }

    @Override
    public CreateBuilder2 orSetData() {
        return orSetData(-1);
    }

    @Override
    public CreateBuilder2 orSetData(int version) {
        setDataIfExists = true;
        setDataIfExistsVersion = version;
        return this;
    }

    @Override
    public CreateBuilder2 idempotent() {
        this.idempotent = true;
        return this;
    }

    @Override
    public CreateBuilderMain withTtl(long ttl) {
        this.ttl = ttl;
        return this;
    }

    <T> TransactionCreateBuilder<T> asTransactionCreateBuilder(
            final T context, final CuratorMultiTransactionRecord transaction) {
        return new TransactionCreateBuilder<T>() {
            @Override
            public PathAndBytesable<T> withACL(List<ACL> aclList) {
                return withACL(aclList, false);
            }

            @Override
            public PathAndBytesable<T> withACL(List<ACL> aclList, boolean applyToParents) {
                CreateBuilderImpl.this.withACL(aclList, applyToParents);
                return this;
            }

            @Override
            public TransactionCreateBuilder2<T> withTtl(long ttl) {
                CreateBuilderImpl.this.withTtl(ttl);
                return this;
            }

            @Override
            public ACLPathAndBytesable<T> withMode(CreateMode mode) {
                CreateBuilderImpl.this.withMode(mode);
                return this;
            }

            @Override
            public ACLCreateModePathAndBytesable<T> compressed() {
                CreateBuilderImpl.this.compressed();
                return this;
            }

            @Override
            public T forPath(String path) throws Exception {
                return forPath(path, client.getDefaultData());
            }

            @Override
            public T forPath(String path, byte[] data) throws Exception {
                if (compress) {
                    data = client.getCompressionProvider().compress(path, data);
                }

                String fixedPath = client.fixForNamespace(path);
                transaction.add(
                        Op.create(fixedPath, data, acling.getAclList(path), createMode, ttl),
                        OperationType.CREATE,
                        path);
                return context;
            }
        };
    }

    @Override
    public CreateBackgroundModeStatACLable compressed() {
        compress = true;
        return new CreateBackgroundModeStatACLable() {
            @Override
            public CreateBackgroundModeACLable storingStatIn(Stat stat) {
                storingStat = stat;
                return asCreateBackgroundModeACLable();
            }

            @Override
            public ACLCreateModePathAndBytesable<String> creatingParentsIfNeeded() {
                createParentsIfNeeded = true;
                return asACLCreateModePathAndBytesable();
            }

            @Override
            public ACLCreateModePathAndBytesable<String> creatingParentContainersIfNeeded() {
                setCreateParentsAsContainers();
                return creatingParentsIfNeeded();
            }

            @Override
            public ACLPathAndBytesable<String> withProtectedEphemeralSequential() {
                return CreateBuilderImpl.this.withProtectedEphemeralSequential();
            }

            @Override
            public BackgroundPathAndBytesable<String> withACL(List<ACL> aclList) {
                return CreateBuilderImpl.this.withACL(aclList);
            }

            @Override
            public BackgroundPathAndBytesable<String> withACL(List<ACL> aclList, boolean applyToParents) {
                return CreateBuilderImpl.this.withACL(aclList, applyToParents);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(BackgroundCallback callback, Object context) {
                return CreateBuilderImpl.this.inBackground(callback, context);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(
                    BackgroundCallback callback, Object context, Executor executor) {
                return CreateBuilderImpl.this.inBackground(callback, context, executor);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground() {
                return CreateBuilderImpl.this.inBackground();
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(Object context) {
                return CreateBuilderImpl.this.inBackground(context);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(BackgroundCallback callback) {
                return CreateBuilderImpl.this.inBackground(callback);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(BackgroundCallback callback, Executor executor) {
                return CreateBuilderImpl.this.inBackground(callback, executor);
            }

            @Override
            public ACLBackgroundPathAndBytesable<String> withMode(CreateMode mode) {
                return CreateBuilderImpl.this.withMode(mode);
            }

            @Override
            public String forPath(String path, byte[] data) throws Exception {
                return CreateBuilderImpl.this.forPath(path, data);
            }

            @Override
            public String forPath(String path) throws Exception {
                return CreateBuilderImpl.this.forPath(path);
            }
        };
    }

    @Override
    public ACLBackgroundPathAndBytesable<String> withACL(List<ACL> aclList) {
        return withACL(aclList, false);
    }

    @Override
    public ACLBackgroundPathAndBytesable<String> withACL(List<ACL> aclList, boolean applyToParents) {
        acling = new ACLing(client.getAclProvider(), aclList, applyToParents);
        return new ACLBackgroundPathAndBytesable<String>() {
            @Override
            public BackgroundPathAndBytesable<String> withACL(List<ACL> aclList) {
                return CreateBuilderImpl.this.withACL(aclList);
            }

            @Override
            public BackgroundPathAndBytesable<String> withACL(List<ACL> aclList, boolean applyToParents) {
                return CreateBuilderImpl.this.withACL(aclList, applyToParents);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground() {
                return CreateBuilderImpl.this.inBackground();
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(BackgroundCallback callback, Object context) {
                return CreateBuilderImpl.this.inBackground(callback, context);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(
                    BackgroundCallback callback, Object context, Executor executor) {
                return CreateBuilderImpl.this.inBackground(callback, context, executor);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(Object context) {
                return CreateBuilderImpl.this.inBackground(context);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(BackgroundCallback callback) {
                return CreateBuilderImpl.this.inBackground(callback);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(BackgroundCallback callback, Executor executor) {
                return CreateBuilderImpl.this.inBackground(callback, executor);
            }

            @Override
            public String forPath(String path, byte[] data) throws Exception {
                return CreateBuilderImpl.this.forPath(path, data);
            }

            @Override
            public String forPath(String path) throws Exception {
                return CreateBuilderImpl.this.forPath(path);
            }
        };
    }

    @Override
    public ProtectACLCreateModeStatPathAndBytesable<String> creatingParentContainersIfNeeded() {
        setCreateParentsAsContainers();
        return creatingParentsIfNeeded();
    }

    private void setCreateParentsAsContainers() {
        if (client.useContainerParentsIfAvailable()) {
            createParentsAsContainers = true;
        }
    }

    @Override
    public ProtectACLCreateModeStatPathAndBytesable<String> creatingParentsIfNeeded() {
        createParentsIfNeeded = true;
        return new ProtectACLCreateModeStatPathAndBytesable<String>() {
            @Override
            public ACLCreateModeBackgroundPathAndBytesable<String> withProtection() {
                return CreateBuilderImpl.this.withProtection();
            }

            @Override
            public BackgroundPathAndBytesable<String> withACL(List<ACL> aclList) {
                return withACL(aclList, false);
            }

            @Override
            public BackgroundPathAndBytesable<String> withACL(List<ACL> aclList, boolean applyToParents) {
                return CreateBuilderImpl.this.withACL(aclList, applyToParents);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground() {
                return CreateBuilderImpl.this.inBackground();
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(Object context) {
                return CreateBuilderImpl.this.inBackground(context);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(BackgroundCallback callback) {
                return CreateBuilderImpl.this.inBackground(callback);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(BackgroundCallback callback, Object context) {
                return CreateBuilderImpl.this.inBackground(callback, context);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(BackgroundCallback callback, Executor executor) {
                return CreateBuilderImpl.this.inBackground(callback, executor);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(
                    BackgroundCallback callback, Object context, Executor executor) {
                return CreateBuilderImpl.this.inBackground(callback, context, executor);
            }

            @Override
            public ACLBackgroundPathAndBytesable<String> withMode(CreateMode mode) {
                return CreateBuilderImpl.this.withMode(mode);
            }

            @Override
            public String forPath(String path, byte[] data) throws Exception {
                return CreateBuilderImpl.this.forPath(path, data);
            }

            @Override
            public String forPath(String path) throws Exception {
                return CreateBuilderImpl.this.forPath(path);
            }

            @Override
            public ACLBackgroundPathAndBytesable<String> storingStatIn(Stat stat) {
                storingStat = stat;
                return CreateBuilderImpl.this;
            }
        };
    }

    @Override
    public ACLCreateModeStatBackgroundPathAndBytesable<String> withProtection() {
        protectedMode.setProtectedMode();
        return asACLCreateModeStatBackgroundPathAndBytesable();
    }

    @Override
    public ACLPathAndBytesable<String> withProtectedEphemeralSequential() {
        protectedMode.setProtectedMode();
        createMode = CreateMode.EPHEMERAL_SEQUENTIAL;

        return new ACLPathAndBytesable<String>() {
            @Override
            public PathAndBytesable<String> withACL(List<ACL> aclList) {
                return CreateBuilderImpl.this.withACL(aclList);
            }

            @Override
            public PathAndBytesable<String> withACL(List<ACL> aclList, boolean applyToParents) {
                return CreateBuilderImpl.this.withACL(aclList, applyToParents);
            }

            @Override
            public String forPath(String path, byte[] data) throws Exception {
                return CreateBuilderImpl.this.forPath(path, data);
            }

            @Override
            public String forPath(String path) throws Exception {
                return CreateBuilderImpl.this.forPath(path);
            }
        };
    }

    @Override
    public ACLBackgroundPathAndBytesable<String> withMode(CreateMode mode) {
        createMode = mode;
        return this;
    }

    @Override
    public ErrorListenerPathAndBytesable<String> inBackground(BackgroundCallback callback, Object context) {
        backgrounding = new Backgrounding(callback, context);
        return this;
    }

    @Override
    public ErrorListenerPathAndBytesable<String> inBackground(
            BackgroundCallback callback, Object context, Executor executor) {
        backgrounding = new Backgrounding(client, callback, context, executor);
        return this;
    }

    @Override
    public ErrorListenerPathAndBytesable<String> inBackground(BackgroundCallback callback) {
        backgrounding = new Backgrounding(callback);
        return this;
    }

    @Override
    public ErrorListenerPathAndBytesable<String> inBackground(BackgroundCallback callback, Executor executor) {
        backgrounding = new Backgrounding(client, callback, executor);
        return this;
    }

    @Override
    public ErrorListenerPathAndBytesable<String> inBackground() {
        backgrounding = new Backgrounding(true);
        return this;
    }

    @Override
    public ErrorListenerPathAndBytesable<String> inBackground(Object context) {
        backgrounding = new Backgrounding(context);
        return this;
    }

    @Override
    public PathAndBytesable<String> withUnhandledErrorListener(UnhandledErrorListener listener) {
        backgrounding = new Backgrounding(backgrounding, listener);
        return this;
    }

    @Override
    public String forPath(String path) throws Exception {
        return forPath(path, client.getDefaultData());
    }

    @Override
    public String forPath(final String givenPath, byte[] data) throws Exception {
        if (compress) {
            data = client.getCompressionProvider().compress(givenPath, data);
        }

        final String adjustedPath = adjustPath(client.fixForNamespace(givenPath, createMode.isSequential()));
        List<ACL> aclList = acling.getAclList(adjustedPath);
        client.getSchemaSet().getSchema(givenPath).validateCreate(createMode, givenPath, data, aclList);

        String returnPath = null;
        if (backgrounding.inBackground()) {
            pathInBackground(adjustedPath, data, givenPath);
        } else {
            String path = protectedPathInForeground(adjustedPath, data, aclList);
            returnPath = client.unfixForNamespace(path);
        }
        return returnPath;
    }

    private String protectedPathInForeground(String adjustedPath, byte[] data, List<ACL> aclList) throws Exception {
        try {
            return pathInForeground(adjustedPath, data, aclList);
        } catch (Exception e) {
            ThreadUtils.checkInterrupted(e);
            if ((e instanceof KeeperException.ConnectionLossException || !(e instanceof KeeperException))
                    && protectedMode.doProtected()) {
                /*
                 * CURATOR-45 + CURATOR-79: we don't know if the create operation was successful or not,
                 * register the znode to be sure it is deleted later.
                 */
                new FindAndDeleteProtectedNodeInBackground(
                                client, ZKPaths.getPathAndNode(adjustedPath).getPath(), protectedMode.protectedId())
                        .execute();
                /*
                 * The current UUID is scheduled to be deleted, it is not safe to use it again.
                 * If this builder is used again later create a new UUID
                 */
                protectedMode.resetProtectedId();
            }

            throw e;
        }
    }

    @Override
    public CuratorEventType getBackgroundEventType() {
        return CuratorEventType.CREATE;
    }

    @Override
    public void performBackgroundOperation(final OperationAndData<PathAndBytes> operationAndData) throws Exception {
        try {
            final OperationTrace trace =
                    client.getZookeeperClient().startAdvancedTracer("CreateBuilderImpl-Background");
            final byte[] data = operationAndData.getData().getData();

            AsyncCallback.Create2Callback callback = new AsyncCallback.Create2Callback() {
                @Override
                public void processResult(int rc, String path, Object ctx, String name, Stat stat) {
                    trace.setReturnCode(rc)
                            .setRequestBytesLength(data)
                            .setPath(path)
                            .commit();

                    if ((stat != null) && (storingStat != null)) {
                        DataTree.copyStat(stat, storingStat);
                    }

                    if ((rc == KeeperException.Code.NONODE.intValue()) && createParentsIfNeeded) {
                        backgroundCreateParentsThenNode(
                                client,
                                operationAndData,
                                operationAndData.getData().getPath(),
                                backgrounding,
                                acling.getACLProviderForParents(),
                                createParentsAsContainers);
                    } else if ((rc == KeeperException.Code.NODEEXISTS.intValue()) && setDataIfExists) {
                        backgroundSetData(
                                client,
                                operationAndData,
                                operationAndData.getData().getPath(),
                                backgrounding);
                    } else if ((rc == KeeperException.Code.NODEEXISTS.intValue()) && idempotent) {
                        backgroundCheckIdempotent(
                                client,
                                operationAndData,
                                operationAndData.getData().getPath(),
                                backgrounding);
                    } else {
                        sendBackgroundResponse(rc, path, ctx, name, stat, operationAndData);
                    }
                }
            };
            client.getZooKeeper()
                    .create(
                            operationAndData.getData().getPath(),
                            data,
                            acling.getAclList(operationAndData.getData().getPath()),
                            createMode,
                            callback,
                            backgrounding.getContext(),
                            ttl);

        } catch (Throwable e) {
            backgrounding.checkError(e, null);
        }
    }

    @Override
    public CreateProtectACLCreateModePathAndBytesable<String> storingStatIn(Stat stat) {
        storingStat = stat;

        return new CreateProtectACLCreateModePathAndBytesable<String>() {

            @Override
            public BackgroundPathAndBytesable<String> withACL(List<ACL> aclList) {
                return CreateBuilderImpl.this.withACL(aclList);
            }

            @Override
            public BackgroundPathAndBytesable<String> withACL(List<ACL> aclList, boolean applyToParents) {
                return CreateBuilderImpl.this.withACL(aclList, applyToParents);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground() {
                return CreateBuilderImpl.this.inBackground();
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(Object context) {
                return CreateBuilderImpl.this.inBackground(context);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(BackgroundCallback callback) {
                return CreateBuilderImpl.this.inBackground(callback);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(BackgroundCallback callback, Object context) {
                return CreateBuilderImpl.this.inBackground(callback, context);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(BackgroundCallback callback, Executor executor) {
                return CreateBuilderImpl.this.inBackground(callback, executor);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(
                    BackgroundCallback callback, Object context, Executor executor) {
                return CreateBuilderImpl.this.inBackground(callback, context, executor);
            }

            @Override
            public String forPath(String path, byte[] data) throws Exception {
                return CreateBuilderImpl.this.forPath(path, data);
            }

            @Override
            public String forPath(String path) throws Exception {
                return CreateBuilderImpl.this.forPath(path);
            }

            @Override
            public ACLBackgroundPathAndBytesable<String> withMode(CreateMode mode) {
                return CreateBuilderImpl.this.withMode(mode);
            }

            @Override
            public ACLCreateModeBackgroundPathAndBytesable<String> withProtection() {
                return CreateBuilderImpl.this.withProtection();
            }

            @Override
            public ProtectACLCreateModePathAndBytesable<String> creatingParentsIfNeeded() {
                return CreateBuilderImpl.this.creatingParentsIfNeeded();
            }

            @Override
            public ProtectACLCreateModePathAndBytesable<String> creatingParentContainersIfNeeded() {
                return CreateBuilderImpl.this.creatingParentContainersIfNeeded();
            }
        };
    }

    static <T> void backgroundCreateParentsThenNode(
            final CuratorFrameworkImpl client,
            final OperationAndData<T> mainOperationAndData,
            final String path,
            Backgrounding backgrounding,
            final InternalACLProvider aclProvider,
            final boolean createParentsAsContainers) {
        BackgroundOperation<T> operation = new BackgroundOperation<T>() {
            @Override
            public void performBackgroundOperation(OperationAndData<T> dummy) throws Exception {
                try {
                    ZKPaths.mkdirs(client.getZooKeeper(), path, false, aclProvider, createParentsAsContainers);
                } catch (KeeperException e) {
                    if (!client.getZookeeperClient().getRetryPolicy().allowRetry(e)) {
                        sendBackgroundResponse(
                                client, e.code().intValue(), e.getPath(), null, null, null, mainOperationAndData);
                        throw e;
                    }
                    // otherwise safe to ignore as it will get retried
                }
                client.queueOperation(mainOperationAndData);
            }

            @Override
            public CuratorEventType getBackgroundEventType() {
                return CuratorEventType.CREATE;
            }
        };
        OperationAndData<T> parentOperation = new OperationAndData<>(
                operation, mainOperationAndData.getData(), null, null, backgrounding.getContext(), null);
        client.queueOperation(parentOperation);
    }

    private void backgroundSetData(
            final CuratorFrameworkImpl client,
            final OperationAndData<PathAndBytes> mainOperationAndData,
            final String path,
            final Backgrounding backgrounding) {
        final AsyncCallback.StatCallback statCallback = new AsyncCallback.StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                if (rc == KeeperException.Code.NONODE.intValue()) {
                    client.queueOperation(mainOperationAndData); // try to create it again
                } else {
                    sendBackgroundResponse(rc, path, ctx, path, stat, mainOperationAndData);
                }
            }
        };
        BackgroundOperation<PathAndBytes> operation = new BackgroundOperation<PathAndBytes>() {
            @Override
            public void performBackgroundOperation(OperationAndData<PathAndBytes> op) throws Exception {
                try {
                    client.getZooKeeper()
                            .setData(
                                    path,
                                    mainOperationAndData.getData().getData(),
                                    setDataIfExistsVersion,
                                    statCallback,
                                    backgrounding.getContext());
                } catch (KeeperException e) {
                    // ignore
                }
            }

            @Override
            public CuratorEventType getBackgroundEventType() {
                return CuratorEventType.CREATE;
            }
        };
        client.queueOperation(new OperationAndData<>(operation, null, null, null, null, null));
    }

    private void backgroundCheckIdempotent(
            final CuratorFrameworkImpl client,
            final OperationAndData<PathAndBytes> mainOperationAndData,
            final String path,
            final Backgrounding backgrounding) {
        final AsyncCallback.DataCallback dataCallback = new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                if (rc == KeeperException.Code.NONODE.intValue()) {
                    client.queueOperation(mainOperationAndData); // try to create it again
                } else {
                    if (rc == KeeperException.Code.OK.intValue()) {
                        if (failNextIdempotentCheckForTesting) {
                            failNextIdempotentCheckForTesting = false;
                            rc = KeeperException.Code.CONNECTIONLOSS.intValue();
                        } else if (!IdempotentUtils.matches(
                                0, mainOperationAndData.getData().getData(), stat.getVersion(), data)) {
                            rc = KeeperException.Code.NODEEXISTS.intValue();
                        }
                    }
                    sendBackgroundResponse(rc, path, ctx, path, stat, mainOperationAndData);
                }
            }
        };
        BackgroundOperation<PathAndBytes> operation = new BackgroundOperation<PathAndBytes>() {
            @Override
            public void performBackgroundOperation(OperationAndData<PathAndBytes> op) throws Exception {
                try {
                    client.getZooKeeper().getData(path, false, dataCallback, backgrounding.getContext());
                } catch (KeeperException e) {
                    // ignore
                    client.logError("Unexpected exception in async idempotent check for, ignoring: " + path, e);
                }
            }

            @Override
            public CuratorEventType getBackgroundEventType() {
                return CuratorEventType.CREATE;
            }
        };
        client.queueOperation(new OperationAndData<>(operation, null, null, null, null, null));
    }

    private void sendBackgroundResponse(
            int rc, String path, Object ctx, String name, Stat stat, OperationAndData<PathAndBytes> operationAndData) {
        sendBackgroundResponse(client, rc, path, ctx, name, stat, operationAndData);
    }

    private static <T> void sendBackgroundResponse(
            CuratorFrameworkImpl client,
            int rc,
            String path,
            Object ctx,
            String name,
            Stat stat,
            OperationAndData<T> operationAndData) {
        CuratorEvent event = new CuratorEventImpl(
                client, CuratorEventType.CREATE, rc, path, name, ctx, stat, null, null, null, null, null);
        client.processBackgroundOperation(operationAndData, event);
    }

    private ACLCreateModePathAndBytesable<String> asACLCreateModePathAndBytesable() {
        return new ACLCreateModePathAndBytesable<String>() {
            @Override
            public PathAndBytesable<String> withACL(List<ACL> aclList) {
                return CreateBuilderImpl.this.withACL(aclList);
            }

            @Override
            public PathAndBytesable<String> withACL(List<ACL> aclList, boolean applyToParents) {
                CreateBuilderImpl.this.withACL(aclList, applyToParents);
                return this;
            }

            @Override
            public ACLPathAndBytesable<String> withMode(CreateMode mode) {
                createMode = mode;
                return new ACLPathAndBytesable<String>() {
                    @Override
                    public PathAndBytesable<String> withACL(List<ACL> aclList) {
                        return CreateBuilderImpl.this.withACL(aclList);
                    }

                    @Override
                    public PathAndBytesable<String> withACL(List<ACL> aclList, boolean applyToParents) {
                        return CreateBuilderImpl.this.withACL(aclList, applyToParents);
                    }

                    @Override
                    public String forPath(String path, byte[] data) throws Exception {
                        return CreateBuilderImpl.this.forPath(path, data);
                    }

                    @Override
                    public String forPath(String path) throws Exception {
                        return CreateBuilderImpl.this.forPath(path);
                    }
                };
            }

            @Override
            public String forPath(String path, byte[] data) throws Exception {
                return CreateBuilderImpl.this.forPath(path, data);
            }

            @Override
            public String forPath(String path) throws Exception {
                return CreateBuilderImpl.this.forPath(path);
            }
        };
    }

    private CreateBackgroundModeACLable asCreateBackgroundModeACLable() {
        return new CreateBackgroundModeACLable() {

            @Override
            public BackgroundPathAndBytesable<String> withACL(List<ACL> aclList) {
                return CreateBuilderImpl.this.withACL(aclList);
            }

            @Override
            public BackgroundPathAndBytesable<String> withACL(List<ACL> aclList, boolean applyToParents) {
                return CreateBuilderImpl.this.withACL(aclList, applyToParents);
            }

            @Override
            public ACLBackgroundPathAndBytesable<String> withMode(CreateMode mode) {
                return CreateBuilderImpl.this.withMode(mode);
            }

            @Override
            public String forPath(String path) throws Exception {
                return CreateBuilderImpl.this.forPath(path);
            }

            @Override
            public String forPath(String path, byte[] data) throws Exception {
                return CreateBuilderImpl.this.forPath(path, data);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(
                    BackgroundCallback callback, Object context, Executor executor) {
                return CreateBuilderImpl.this.inBackground(callback, context, executor);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(BackgroundCallback callback, Executor executor) {
                return CreateBuilderImpl.this.inBackground(callback, executor);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(BackgroundCallback callback, Object context) {
                return CreateBuilderImpl.this.inBackground(callback, context);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(BackgroundCallback callback) {
                return CreateBuilderImpl.this.inBackground(callback);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(Object context) {
                return CreateBuilderImpl.this.inBackground(context);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground() {
                return CreateBuilderImpl.this.inBackground();
            }

            @Override
            public ACLPathAndBytesable<String> withProtectedEphemeralSequential() {
                return CreateBuilderImpl.this.withProtectedEphemeralSequential();
            }

            @Override
            public ACLCreateModePathAndBytesable<String> creatingParentsIfNeeded() {
                createParentsIfNeeded = true;
                return asACLCreateModePathAndBytesable();
            }

            @Override
            public ACLCreateModePathAndBytesable<String> creatingParentContainersIfNeeded() {
                setCreateParentsAsContainers();
                return asACLCreateModePathAndBytesable();
            }
        };
    }

    private ACLCreateModeStatBackgroundPathAndBytesable<String> asACLCreateModeStatBackgroundPathAndBytesable() {
        return new ACLCreateModeStatBackgroundPathAndBytesable<String>() {
            @Override
            public BackgroundPathAndBytesable<String> withACL(List<ACL> aclList) {
                return CreateBuilderImpl.this.withACL(aclList);
            }

            @Override
            public BackgroundPathAndBytesable<String> withACL(List<ACL> aclList, boolean applyToParents) {
                CreateBuilderImpl.this.withACL(aclList, applyToParents);
                return this;
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground() {
                return CreateBuilderImpl.this.inBackground();
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(
                    BackgroundCallback callback, Object context, Executor executor) {
                return CreateBuilderImpl.this.inBackground(callback, context, executor);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(BackgroundCallback callback, Executor executor) {
                return CreateBuilderImpl.this.inBackground(callback, executor);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(BackgroundCallback callback, Object context) {
                return CreateBuilderImpl.this.inBackground(callback, context);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(BackgroundCallback callback) {
                return CreateBuilderImpl.this.inBackground(callback);
            }

            @Override
            public ErrorListenerPathAndBytesable<String> inBackground(Object context) {
                return CreateBuilderImpl.this.inBackground(context);
            }

            @Override
            public String forPath(String path) throws Exception {
                return CreateBuilderImpl.this.forPath(path);
            }

            @Override
            public String forPath(String path, byte[] data) throws Exception {
                return CreateBuilderImpl.this.forPath(path, data);
            }

            @Override
            public ACLBackgroundPathAndBytesable<String> withMode(CreateMode mode) {
                return CreateBuilderImpl.this.withMode(mode);
            }

            @Override
            public ACLCreateModeBackgroundPathAndBytesable<String> storingStatIn(Stat stat) {
                storingStat = stat;
                return CreateBuilderImpl.this;
            }
        };
    }

    @VisibleForTesting
    volatile boolean debugForceFindProtectedNode = false;

    private void pathInBackground(final String path, final byte[] data, final String givenPath) {
        final AtomicBoolean firstTime = new AtomicBoolean(true);
        OperationAndData<PathAndBytes> operationAndData =
                new OperationAndData<PathAndBytes>(
                        this,
                        new PathAndBytes(path, data),
                        backgrounding.getCallback(),
                        new OperationAndData.ErrorCallback<PathAndBytes>() {
                            public void retriesExhausted(OperationAndData<PathAndBytes> operationAndData) {
                                if (protectedMode.doProtected()) {
                                    // all retries have failed, findProtectedNodeInForeground(..) included, schedule a
                                    // clean up
                                    new FindAndDeleteProtectedNodeInBackground(
                                                    client,
                                                    ZKPaths.getPathAndNode(path).getPath(),
                                                    protectedMode.protectedId())
                                            .execute();
                                    // assign a new id if this builder is used again later
                                    protectedMode.resetProtectedId();
                                }
                            }
                        },
                        backgrounding.getContext(),
                        null) {
                    @Override
                    void callPerformBackgroundOperation() throws Exception {
                        boolean callSuper = true;
                        boolean localFirstTime = firstTime.getAndSet(false) && !debugForceFindProtectedNode;

                        protectedMode.checkSetSessionId(client, createMode);
                        if (!localFirstTime && protectedMode.doProtected()) {
                            debugForceFindProtectedNode = false;
                            String createdPath = null;
                            try {
                                createdPath = findProtectedNodeInForeground(path);
                            } catch (KeeperException.ConnectionLossException e) {
                                sendBackgroundResponse(
                                        KeeperException.Code.CONNECTIONLOSS.intValue(),
                                        path,
                                        backgrounding.getContext(),
                                        null,
                                        null,
                                        this);
                                callSuper = false;
                            }
                            if (createdPath != null) {
                                try {
                                    sendBackgroundResponse(
                                            KeeperException.Code.OK.intValue(),
                                            createdPath,
                                            backgrounding.getContext(),
                                            createdPath,
                                            null,
                                            this);
                                } catch (Exception e) {
                                    ThreadUtils.checkInterrupted(e);
                                    client.logError("Processing protected create for path: " + givenPath, e);
                                }
                                callSuper = false;
                            }
                        }

                        if (failBeforeNextCreateForTesting) {
                            failBeforeNextCreateForTesting = false;
                            throw new KeeperException.ConnectionLossException();
                        }

                        if (failNextCreateForTesting) {
                            failNextCreateForTesting = false;
                            try {
                                // simulate success on server without notification to client
                                // if another error occurs in pathInForeground that isn't NodeExists, this hangs instead
                                // of fully propagating the error. Likely not worth fixing though.
                                pathInForeground(path, data, acling.getAclList(path));
                            } catch (KeeperException.NodeExistsException e) {
                                client.logError(
                                        "NodeExists while injecting failure after create, ignoring: " + givenPath, e);
                            }
                            throw new KeeperException.ConnectionLossException();
                        }

                        if (callSuper) {
                            super.callPerformBackgroundOperation();
                        }
                    }
                };
        client.processBackgroundOperation(operationAndData, null);
    }

    private String pathInForeground(final String path, final byte[] data, final List<ACL> aclList) throws Exception {
        OperationTrace trace = client.getZookeeperClient().startAdvancedTracer("CreateBuilderImpl-Foreground");

        final AtomicBoolean firstTime = new AtomicBoolean(true);
        String returnPath = RetryLoop.callWithRetry(client.getZookeeperClient(), new Callable<String>() {
            @Override
            public String call() throws Exception {
                boolean localFirstTime = firstTime.getAndSet(false) && !debugForceFindProtectedNode;
                protectedMode.checkSetSessionId(client, createMode);

                String createdPath = null;
                if (!localFirstTime && protectedMode.doProtected()) {
                    debugForceFindProtectedNode = false;
                    createdPath = findProtectedNodeInForeground(path);
                }

                if (createdPath == null) {
                    try {
                        if (failBeforeNextCreateForTesting) {
                            failBeforeNextCreateForTesting = false;
                            throw new KeeperException.ConnectionLossException();
                        }
                        createdPath = client.getZooKeeper().create(path, data, aclList, createMode, storingStat, ttl);
                    } catch (KeeperException.NoNodeException e) {
                        if (createParentsIfNeeded) {
                            ZKPaths.mkdirs(
                                    client.getZooKeeper(),
                                    path,
                                    false,
                                    acling.getACLProviderForParents(),
                                    createParentsAsContainers);
                            createdPath = client.getZooKeeper()
                                    .create(path, data, acling.getAclList(path), createMode, storingStat, ttl);
                        } else {
                            throw e;
                        }
                    } catch (KeeperException.NodeExistsException e) {
                        if (setDataIfExists) {
                            Stat setStat = client.getZooKeeper().setData(path, data, setDataIfExistsVersion);
                            if (storingStat != null) {
                                DataTree.copyStat(setStat, storingStat);
                            }
                            createdPath = path;
                        } else if (idempotent) {
                            if (failNextIdempotentCheckForTesting) {
                                failNextIdempotentCheckForTesting = false;
                                throw new KeeperException.ConnectionLossException();
                            }
                            Stat getStat = new Stat();
                            byte[] existingData = client.getZooKeeper().getData(path, false, getStat);
                            // check to see if data version == 0 and data matches the idempotent case
                            if (IdempotentUtils.matches(0, data, getStat.getVersion(), existingData)) {
                                if (storingStat != null) {
                                    DataTree.copyStat(getStat, storingStat);
                                }
                                createdPath = path;
                            } else {
                                throw e;
                            }
                        } else {
                            throw e;
                        }
                    }
                }

                if (failNextCreateForTesting) {
                    failNextCreateForTesting = false;
                    throw new KeeperException.ConnectionLossException();
                }
                return createdPath;
            }
        });

        trace.setRequestBytesLength(data).setPath(path).commit();
        return returnPath;
    }

    private String findProtectedNodeInForeground(final String path) throws Exception {
        OperationTrace trace =
                client.getZookeeperClient().startAdvancedTracer("CreateBuilderImpl-findProtectedNodeInForeground");

        String returnPath = RetryLoop.callWithRetry(client.getZookeeperClient(), new Callable<String>() {
            @Override
            public String call() throws Exception {
                String foundNode = null;
                try {
                    final ZKPaths.PathAndNode pathAndNode = ZKPaths.getPathAndNode(path);
                    List<String> children = client.getZooKeeper().getChildren(pathAndNode.getPath(), false);

                    foundNode = findNode(children, pathAndNode.getPath(), protectedMode.protectedId());
                    log.debug("Protected mode findNode result: {}", foundNode);

                    foundNode = protectedMode.validateFoundNode(client, createMode, foundNode);
                } catch (KeeperException.NoNodeException ignore) {
                    // ignore
                }
                return foundNode;
            }
        });

        trace.setPath(path).commit();
        return returnPath;
    }

    @VisibleForTesting
    String adjustPath(String path) throws Exception {
        return ProtectedUtils.toProtectedZNodePath(path, protectedMode.protectedId());
    }

    /**
     * Attempt to find the znode that matches the given path and protected id
     *
     * @param children    a list of candidates znodes
     * @param path        the path
     * @param protectedId the protected id
     * @return the absolute path of the znode or <code>null</code> if it is not found
     */
    static String findNode(final List<String> children, final String path, final String protectedId) {
        final String protectedPrefix = ProtectedUtils.getProtectedPrefix(protectedId);
        String foundNode = Iterables.find(
                children,
                new Predicate<String>() {
                    @Override
                    public boolean apply(String node) {
                        return node.startsWith(protectedPrefix);
                    }
                },
                null);
        if (foundNode != null) {
            foundNode = ZKPaths.makePath(path, foundNode);
        }
        return foundNode;
    }
}
