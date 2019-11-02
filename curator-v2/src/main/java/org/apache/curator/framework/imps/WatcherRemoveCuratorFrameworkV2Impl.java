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

import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.WatcherRemoveCuratorFramework;
import org.apache.curator.framework.api.*;
import org.apache.curator.framework.api.transaction.CuratorMultiTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.TransactionOp;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.schema.SchemaSet;
import org.apache.curator.framework.state.ConnectionStateErrorPolicy;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.v2.CuratorFrameworkV2;
import org.apache.curator.v2.WatcherRemoveCuratorFrameworkV2;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class WatcherRemoveCuratorFrameworkV2Impl implements WatcherRemoveCuratorFrameworkV2
{
    private final CuratorFrameworkImpl client;
    private final WatcherRemoveCuratorFramework facade;

    WatcherRemoveCuratorFrameworkV2Impl(CuratorFrameworkImpl client, WatcherRemoveCuratorFramework facade)
    {
        this.client = client;
        this.facade = facade;
    }

    @Override
    public void removeWatchers()
    {
        facade.removeWatchers();
    }

    @Override
    public void start()
    {
        facade.start();
    }

    @Override
    public void close()
    {
        facade.close();
    }

    @Override
    public CuratorFrameworkState getState()
    {
        return facade.getState();
    }

    @Override
    public boolean isStarted()
    {
        throw new UnsupportedOperationException("deprecated");
    }

    @Override
    public CreateBuilder create()
    {
        return facade.create();
    }

    @Override
    public DeleteBuilder delete()
    {
        return facade.delete();
    }

    @Override
    public ExistsBuilder checkExists()
    {
        return facade.checkExists();
    }

    @Override
    public GetDataBuilder getData()
    {
        return facade.getData();
    }

    @Override
    public SetDataBuilder setData()
    {
        return facade.setData();
    }

    @Override
    public GetChildrenBuilder getChildren()
    {
        return facade.getChildren();
    }

    @Override
    public GetACLBuilder getACL()
    {
        return facade.getACL();
    }

    @Override
    public SetACLBuilder setACL()
    {
        return facade.setACL();
    }

    @Override
    public ReconfigBuilder reconfig()
    {
        return facade.reconfig();
    }

    @Override
    public GetConfigBuilder getConfig()
    {
        return facade.getConfig();
    }

    @Override
    public CuratorTransaction inTransaction()
    {
        throw new UnsupportedOperationException("deprecated");
    }

    @Override
    public CuratorMultiTransaction transaction()
    {
        return facade.transaction();
    }

    @Override
    public TransactionOp transactionOp()
    {
        return facade.transactionOp();
    }

    @Override
    public void sync(String path, Object backgroundContextObject)
    {
        throw new UnsupportedOperationException("deprecated");
    }

    @Override
    public void createContainers(String path) throws Exception
    {
        facade.createContainers(path);
    }

    @Override
    public SyncBuilder sync()
    {
        return facade.sync();
    }

    @Override
    public WatchesBuilder watches()
    {
        return new WatchesBuilderImpl(client);
    }

    @Override
    public Listenable<ConnectionStateListener> getConnectionStateListenable()
    {
        return facade.getConnectionStateListenable();
    }

    @Override
    public Listenable<CuratorListener> getCuratorListenable()
    {
        return facade.getCuratorListenable();
    }

    @Override
    public Listenable<UnhandledErrorListener> getUnhandledErrorListenable()
    {
        return facade.getUnhandledErrorListenable();
    }

    @Override
    public CuratorFramework nonNamespaceView()
    {
        throw new UnsupportedOperationException("deprecated");
    }

    @Override
    public CuratorFrameworkV2 usingNamespace(String newNamespace)
    {
        return CuratorFrameworkV2.wrap(facade.usingNamespace(newNamespace));
    }

    @Override
    public String getNamespace()
    {
        return facade.getNamespace();
    }

    @Override
    public CuratorZookeeperClient getZookeeperClient()
    {
        return facade.getZookeeperClient();
    }

    @Override
    public EnsurePath newNamespaceAwareEnsurePath(String path)
    {
        throw new UnsupportedOperationException("deprecated");
    }

    @Override
    public void clearWatcherReferences(Watcher watcher)
    {
        throw new UnsupportedOperationException("deprecated");
    }

    @Override
    public boolean blockUntilConnected(int maxWaitTime, TimeUnit units) throws InterruptedException
    {
        return facade.blockUntilConnected(maxWaitTime, units);
    }

    @Override
    public void blockUntilConnected() throws InterruptedException
    {
        facade.blockUntilConnected();
    }

    @Override
    public WatcherRemoveCuratorFrameworkV2 newWatcherRemoveCuratorFramework()
    {
        return new WatcherRemoveCuratorFrameworkV2Impl(client, facade.newWatcherRemoveCuratorFramework());
    }

    @Override
    public ConnectionStateErrorPolicy getConnectionStateErrorPolicy()
    {
        return facade.getConnectionStateErrorPolicy();
    }

    @Override
    public QuorumVerifier getCurrentConfig()
    {
        return facade.getCurrentConfig();
    }

    @Override
    public SchemaSet getSchemaSet()
    {
        return facade.getSchemaSet();
    }

    @Override
    public boolean isZk34CompatibilityMode()
    {
        return facade.isZk34CompatibilityMode();
    }

    @Override
    public CompletableFuture<Void> runSafe(Runnable runnable)
    {
        return facade.runSafe(runnable);
    }
}
