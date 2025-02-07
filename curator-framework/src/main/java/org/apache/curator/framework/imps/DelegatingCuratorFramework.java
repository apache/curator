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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.api.CompressionProvider;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.schema.SchemaSet;
import org.apache.curator.framework.state.ConnectionStateErrorPolicy;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.ZookeeperCompatibility;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;

/**
 * Delegates methods to shadowed {@link CuratorFrameworkBase} so subclasses can override only methods that need
 * additional cares.
 */
abstract class DelegatingCuratorFramework extends CuratorFrameworkBase {
    protected final CuratorFrameworkBase client;

    public DelegatingCuratorFramework(CuratorFrameworkBase client) {
        this.client = client;
    }

    @Override
    public CuratorFrameworkState getState() {
        return client.getState();
    }

    @Override
    @Deprecated
    public boolean isStarted() {
        return client.isStarted();
    }

    @Override
    public Listenable<ConnectionStateListener> getConnectionStateListenable() {
        return client.getConnectionStateListenable();
    }

    @Override
    public Listenable<CuratorListener> getCuratorListenable() {
        return client.getCuratorListenable();
    }

    @Override
    public Listenable<UnhandledErrorListener> getUnhandledErrorListenable() {
        return client.getUnhandledErrorListenable();
    }

    @Override
    public CuratorFramework usingNamespace(String newNamespace) {
        return client.usingNamespace(newNamespace);
    }

    @Override
    public CuratorZookeeperClient getZookeeperClient() {
        return client.getZookeeperClient();
    }

    @Override
    public ZookeeperCompatibility getZookeeperCompatibility() {
        return client.getZookeeperCompatibility();
    }

    @Override
    @Deprecated
    public void clearWatcherReferences(Watcher watcher) {
        client.clearWatcherReferences(watcher);
    }

    @Override
    public boolean blockUntilConnected(int maxWaitTime, TimeUnit units) throws InterruptedException {
        return client.blockUntilConnected(maxWaitTime, units);
    }

    @Override
    public void blockUntilConnected() throws InterruptedException {
        client.blockUntilConnected();
    }

    @Override
    public ConnectionStateErrorPolicy getConnectionStateErrorPolicy() {
        return client.getConnectionStateErrorPolicy();
    }

    @Override
    public QuorumVerifier getCurrentConfig() {
        return client.getCurrentConfig();
    }

    @Override
    public SchemaSet getSchemaSet() {
        return client.getSchemaSet();
    }

    @Override
    public CompletableFuture<Void> postSafeNotify(Object monitorHolder) {
        return client.postSafeNotify(monitorHolder);
    }

    @Override
    public CompletableFuture<Void> runSafe(Runnable runnable) {
        return client.runSafe(runnable);
    }

    @Override
    NamespaceImpl getNamespaceImpl() {
        return client.getNamespaceImpl();
    }

    @Override
    void validateConnection(Watcher.Event.KeeperState state) {
        client.validateConnection(state);
    }

    @Override
    WatcherRemovalManager getWatcherRemovalManager() {
        return client.getWatcherRemovalManager();
    }

    @Override
    FailedDeleteManager getFailedDeleteManager() {
        return client.getFailedDeleteManager();
    }

    @Override
    FailedRemoveWatchManager getFailedRemoveWatcherManager() {
        return client.getFailedRemoveWatcherManager();
    }

    @Override
    public void logError(String reason, Throwable e) {
        client.logError(reason, e);
    }

    @Override
    byte[] getDefaultData() {
        return client.getDefaultData();
    }

    @Override
    CompressionProvider getCompressionProvider() {
        return client.getCompressionProvider();
    }

    @Override
    public boolean compressionEnabled() {
        return client.compressionEnabled();
    }

    @Override
    ACLProvider getAclProvider() {
        return client.getAclProvider();
    }

    @Override
    boolean useContainerParentsIfAvailable() {
        return client.useContainerParentsIfAvailable();
    }

    @Override
    EnsembleTracker getEnsembleTracker() {
        return client.getEnsembleTracker();
    }

    @Override
    NamespaceFacadeCache getNamespaceFacadeCache() {
        return client.getNamespaceFacadeCache();
    }

    @Override
    <DATA_TYPE> void processBackgroundOperation(OperationAndData<DATA_TYPE> operationAndData, CuratorEvent event) {
        client.processBackgroundOperation(operationAndData, event);
    }

    @Override
    <DATA_TYPE> boolean queueOperation(OperationAndData<DATA_TYPE> operationAndData) {
        return client.queueOperation(operationAndData);
    }
}
