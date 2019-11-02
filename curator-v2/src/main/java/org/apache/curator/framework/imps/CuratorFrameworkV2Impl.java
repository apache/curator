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
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class CuratorFrameworkV2Impl implements CuratorFrameworkV2
{
    private final CuratorFrameworkImpl client;

    public CuratorFrameworkV2Impl(CuratorFramework client)
    {
        this.client = reveal(client);
    }

    private static CuratorFrameworkImpl reveal(CuratorFramework client)
    {
        try
        {
            return (CuratorFrameworkImpl)Objects.requireNonNull(client, "client cannot be null");
        }
        catch ( Exception e )
        {
            throw new IllegalArgumentException("Only Curator clients created through CuratorFrameworkFactory are supported: " + client.getClass().getName());
        }
    }

    @Override
    public void start()
    {
        client.start();
    }

    @Override
    public void close()
    {
        client.close();
    }

    @Override
    public CuratorFrameworkState getState()
    {
        return client.getState();
    }

    @Override
    public boolean isStarted()
    {
        throw new UnsupportedOperationException("deprecated");
    }

    @Override
    public CreateBuilder create()
    {
        return client.create();
    }

    @Override
    public DeleteBuilder delete()
    {
        return client.delete();
    }

    @Override
    public ExistsBuilder checkExists()
    {
        return client.checkExists();
    }

    @Override
    public GetDataBuilder getData()
    {
        return client.getData();
    }

    @Override
    public SetDataBuilder setData()
    {
        return client.setData();
    }

    @Override
    public GetChildrenBuilder getChildren()
    {
        return client.getChildren();
    }

    @Override
    public GetACLBuilder getACL()
    {
        return client.getACL();
    }

    @Override
    public SetACLBuilder setACL()
    {
        return client.setACL();
    }

    @Override
    public ReconfigBuilder reconfig()
    {
        return client.reconfig();
    }

    @Override
    public GetConfigBuilder getConfig()
    {
        return client.getConfig();
    }

    @SuppressWarnings("deprecation")
    @Override
    public CuratorTransaction inTransaction()
    {
        throw new UnsupportedOperationException("deprecated");
    }

    @Override
    public CuratorMultiTransaction transaction()
    {
        return client.transaction();
    }

    @Override
    public TransactionOp transactionOp()
    {
        return client.transactionOp();
    }

    @Override
    public void sync(String path, Object backgroundContextObject)
    {
        throw new UnsupportedOperationException("deprecated");
    }

    @Override
    public void createContainers(String path) throws Exception
    {
        client.createContainers(path);
    }

    @Override
    public SyncBuilder sync()
    {
        return client.sync();
    }

    @Override
    public WatchesBuilder watches()
    {
        return new WatchesBuilderImpl(client);
    }

    @Override
    public Listenable<ConnectionStateListener> getConnectionStateListenable()
    {
        return client.getConnectionStateListenable();
    }

    @Override
    public Listenable<CuratorListener> getCuratorListenable()
    {
        return client.getCuratorListenable();
    }

    @Override
    public Listenable<UnhandledErrorListener> getUnhandledErrorListenable()
    {
        return client.getUnhandledErrorListenable();
    }

    @Override
    public CuratorFramework nonNamespaceView()
    {
        throw new UnsupportedOperationException("deprecated");
    }

    @Override
    public CuratorFrameworkV2 usingNamespace(String newNamespace)
    {
        return CuratorFrameworkV2.wrap(client.usingNamespace(newNamespace));
    }

    @Override
    public String getNamespace()
    {
        return client.getNamespace();
    }

    @Override
    public CuratorZookeeperClient getZookeeperClient()
    {
        return client.getZookeeperClient();
    }

    @SuppressWarnings("deprecation")
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
        return client.blockUntilConnected(maxWaitTime, units);
    }

    @Override
    public void blockUntilConnected() throws InterruptedException
    {
        client.blockUntilConnected();
    }

    @Override
    public WatcherRemoveCuratorFrameworkV2 newWatcherRemoveCuratorFramework()
    {
        return new WatcherRemoveCuratorFrameworkV2Impl(client, client.newWatcherRemoveCuratorFramework());
    }

    @Override
    public ConnectionStateErrorPolicy getConnectionStateErrorPolicy()
    {
        return client.getConnectionStateErrorPolicy();
    }

    @Override
    public QuorumVerifier getCurrentConfig()
    {
        return client.getCurrentConfig();
    }

    @Override
    public SchemaSet getSchemaSet()
    {
        return client.getSchemaSet();
    }

    @Override
    public boolean isZk34CompatibilityMode()
    {
        return client.isZk34CompatibilityMode();
    }

    @Override
    public CompletableFuture<Void> runSafe(Runnable runnable)
    {
        return client.runSafe(runnable);
    }
}
