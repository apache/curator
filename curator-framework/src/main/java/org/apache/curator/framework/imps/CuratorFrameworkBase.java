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

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.WatcherRemoveCuratorFramework;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.api.CompressionProvider;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.CuratorClosedException;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.curator.framework.api.ExistsBuilder;
import org.apache.curator.framework.api.GetACLBuilder;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.api.GetConfigBuilder;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.framework.api.ReconfigBuilder;
import org.apache.curator.framework.api.RemoveWatchesBuilder;
import org.apache.curator.framework.api.SetACLBuilder;
import org.apache.curator.framework.api.SetDataBuilder;
import org.apache.curator.framework.api.SyncBuilder;
import org.apache.curator.framework.api.WatchesBuilder;
import org.apache.curator.framework.api.transaction.CuratorMultiTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.TransactionOp;
import org.apache.curator.utils.EnsurePath;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * This is the base class of all {@link CuratorFramework}s, it is public for private usages (a.k.a. impls/details package).
 *
 * <p>Most internal codes should use {@link CuratorFrameworkBase} instead of {@link CuratorFrameworkImpl}, so
 * functionalities could be added additively by overriding methods in {@link DelegatingCuratorFramework}.
 *
 * <p>An instance of {@link CuratorFramework} MUST BE an instance of {@link CuratorFrameworkBase}.
 */
public abstract class CuratorFrameworkBase implements CuratorFramework {
    abstract NamespaceImpl getNamespaceImpl();

    @Override
    public final CuratorFramework nonNamespaceView() {
        return usingNamespace(null);
    }

    @Override
    public final String getNamespace() {
        NamespaceImpl namespace = getNamespaceImpl();
        String str = namespace.getNamespace();
        return (str != null) ? str : "";
    }

    @Deprecated
    @Override
    public EnsurePath newNamespaceAwareEnsurePath(String path) {
        NamespaceImpl namespace = getNamespaceImpl();
        return namespace.newNamespaceAwareEnsurePath(path);
    }

    final String unfixForNamespace(String path) {
        NamespaceImpl namespace = getNamespaceImpl();
        return namespace.unfixForNamespace(path);
    }

    final String fixForNamespace(String path) {
        NamespaceImpl namespace = getNamespaceImpl();
        return namespace.fixForNamespace(path, false);
    }

    abstract void validateConnection(Watcher.Event.KeeperState state);

    final String fixForNamespace(String path, boolean isSequential) {
        NamespaceImpl namespace = getNamespaceImpl();
        return namespace.fixForNamespace(path, isSequential);
    }

    protected final void checkState() {
        CuratorFrameworkState state = getState();
        switch (state) {
            case STARTED:
                return;
            case STOPPED:
                throw new CuratorClosedException();
            default:
                String msg = String.format("Expected state [%s] was [%s]", CuratorFrameworkState.STARTED, state);
                throw new IllegalStateException(msg);
        }
    }

    final ZooKeeper getZooKeeper() throws Exception {
        return getZookeeperClient().getZooKeeper();
    }

    protected final void internalSync(CuratorFrameworkBase impl, String path, Object context) {
        BackgroundOperation<String> operation = new BackgroundSyncImpl(impl, context);
        processBackgroundOperation(new OperationAndData(operation, path, null, null, context, null), null);
    }

    abstract byte[] getDefaultData();

    abstract CompressionProvider getCompressionProvider();

    abstract ACLProvider getAclProvider();

    abstract boolean useContainerParentsIfAvailable();

    abstract EnsembleTracker getEnsembleTracker();

    abstract NamespaceFacadeCache getNamespaceFacadeCache();

    @Override
    public CreateBuilder create() {
        checkState();
        return new CreateBuilderImpl(this);
    }

    @Override
    public DeleteBuilder delete() {
        checkState();
        return new DeleteBuilderImpl(this);
    }

    @Override
    public ExistsBuilder checkExists() {
        checkState();
        return new ExistsBuilderImpl(this);
    }

    @Override
    public GetDataBuilder getData() {
        checkState();
        return new GetDataBuilderImpl(this);
    }

    @Override
    public SetDataBuilder setData() {
        checkState();
        return new SetDataBuilderImpl(this);
    }

    @Override
    public GetChildrenBuilder getChildren() {
        checkState();
        return new GetChildrenBuilderImpl(this);
    }

    @Override
    public GetACLBuilder getACL() {
        checkState();
        return new GetACLBuilderImpl(this);
    }

    @Override
    public SetACLBuilder setACL() {
        checkState();
        return new SetACLBuilderImpl(this);
    }

    @Override
    public ReconfigBuilder reconfig() {
        checkState();
        return new ReconfigBuilderImpl(this);
    }

    @Override
    public GetConfigBuilder getConfig() {
        checkState();
        return new GetConfigBuilderImpl(this);
    }

    @Override
    public CuratorTransaction inTransaction() {
        checkState();
        return new CuratorTransactionImpl(this);
    }

    @Override
    public CuratorMultiTransaction transaction() {
        checkState();
        return new CuratorMultiTransactionImpl(this);
    }

    @Override
    public TransactionOp transactionOp() {
        checkState();
        return new TransactionOpImpl(this);
    }

    @Override
    public void sync(String path, Object context) {
        checkState();

        path = fixForNamespace(path);

        internalSync(this, path, context);
    }

    @Override
    public SyncBuilder sync() {
        checkState();
        return new SyncBuilderImpl(this);
    }

    @Override
    public final RemoveWatchesBuilder watches() {
        checkState();
        return new RemoveWatchesBuilderImpl(this);
    }

    @Override
    public final WatchesBuilder watchers() {
        Preconditions.checkState(
                getZookeeperCompatibility().hasPersistentWatchers(),
                "watchers() is not supported in the ZooKeeper library and/or server being used. Use watches() instead.");
        checkState();
        return new WatchesBuilderImpl(this);
    }

    @Override
    public final void createContainers(String path) throws Exception {
        checkExists().creatingParentContainersIfNeeded().forPath(ZKPaths.makePath(path, "foo"));
    }

    @Override
    public final WatcherRemoveCuratorFramework newWatcherRemoveCuratorFramework() {
        return new WatcherRemovalFacade(this);
    }

    WatcherRemovalManager getWatcherRemovalManager() {
        return null;
    }

    abstract FailedDeleteManager getFailedDeleteManager();

    abstract FailedRemoveWatchManager getFailedRemoveWatcherManager();

    abstract void logError(String reason, Throwable e);

    abstract <DATA_TYPE> void processBackgroundOperation(
            OperationAndData<DATA_TYPE> operationAndData, CuratorEvent event);

    abstract <DATA_TYPE> boolean queueOperation(OperationAndData<DATA_TYPE> operationAndData);
}
