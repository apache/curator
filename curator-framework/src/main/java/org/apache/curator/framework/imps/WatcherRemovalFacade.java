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
import org.apache.curator.RetryLoop;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.WatcherRemoveCuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.ZooKeeper;

class WatcherRemovalFacade extends CuratorFrameworkImpl implements WatcherRemoveCuratorFramework
{
    private final CuratorFrameworkImpl client;
    private final WatcherRemovalManager removalManager;

    WatcherRemovalFacade(CuratorFrameworkImpl client)
    {
        super(client);
        this.client = client;
        removalManager = new WatcherRemovalManager(client);
    }

    @Override
    public WatcherRemoveCuratorFramework newWatcherRemoveCuratorFramework()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeWatchers()
    {
        removalManager.removeWatchers();
    }

    @Override
    WatcherRemovalManager getWatcherRemovalManager()
    {
        return removalManager;
    }

    @Override
    public CuratorFramework nonNamespaceView()
    {
        return client.usingNamespace(null);
    }

    @Override
    public CuratorFramework usingNamespace(String newNamespace)
    {
        return client.getNamespaceFacadeCache().get(newNamespace);
    }

    @Override
    public String getNamespace()
    {
        return client.getNamespace();
    }

    @Override
    public void start()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close()
    {
        throw new UnsupportedOperationException();
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
    public void sync(String path, Object context)
    {
        client.sync(path, context);
    }

    @Override
    public CuratorZookeeperClient getZookeeperClient()
    {
        return client.getZookeeperClient();
    }

    @Override
    RetryLoop newRetryLoop()
    {
        return client.newRetryLoop();
    }

    @Override
    ZooKeeper getZooKeeper() throws Exception
    {
        return client.getZooKeeper();
    }

    @Override
    <DATA_TYPE> void processBackgroundOperation(OperationAndData<DATA_TYPE> operationAndData, CuratorEvent event)
    {
        client.processBackgroundOperation(operationAndData, event);
    }

    @Override
    void logError(String reason, Throwable e)
    {
        client.logError(reason, e);
    }

    @Override
    String unfixForNamespace(String path)
    {
        return client.unfixForNamespace(path);
    }

    @Override
    String fixForNamespace(String path)
    {
        return client.fixForNamespace(path);
    }

    @Override
    public EnsurePath newNamespaceAwareEnsurePath(String path)
    {
        return client.newNamespaceAwareEnsurePath(path);
    }

    @Override
    FailedDeleteManager getFailedDeleteManager()
    {
        return client.getFailedDeleteManager();
    }
}
