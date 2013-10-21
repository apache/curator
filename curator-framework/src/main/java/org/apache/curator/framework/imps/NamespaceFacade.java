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
import org.apache.curator.framework.api.*;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.ZooKeeper;

class NamespaceFacade extends CuratorFrameworkImpl
{
    private final CuratorFrameworkImpl client;
    private final NamespaceImpl namespace;
    private final FailedDeleteManager failedDeleteManager = new FailedDeleteManager(this);

    NamespaceFacade(CuratorFrameworkImpl client, String namespace)
    {
        super(client);
        this.client = client;
        this.namespace = new NamespaceImpl(client, namespace);
    }

    @Override
    public CuratorFramework nonNamespaceView()
    {
        return usingNamespace(null);
    }

    @Override
    public CuratorFramework usingNamespace(String newNamespace)
    {
        return client.getNamespaceFacadeCache().get(newNamespace);
    }

    @Override
    public String getNamespace()
    {
        return namespace.getNamespace();
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
    public Listenable<ConnectionStateListener> getConnectionStateListenable()
    {
        return client.getConnectionStateListenable();
    }

    @Override
    public Listenable<CuratorListener> getCuratorListenable()
    {
        throw new UnsupportedOperationException("getCuratorListenable() is only available from a non-namespaced CuratorFramework instance");
    }

    @Override
    public Listenable<UnhandledErrorListener> getUnhandledErrorListenable()
    {
        return client.getUnhandledErrorListenable();
    }

    @Override
    public void sync(String path, Object context)
    {
        internalSync(this, path, context);
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
        return namespace.unfixForNamespace(path);
    }

    @Override
    String fixForNamespace(String path)
    {
        return namespace.fixForNamespace(path);
    }

    @Override
    public EnsurePath newNamespaceAwareEnsurePath(String path)
    {
        return namespace.newNamespaceAwareEnsurePath(path);
    }

    @Override
    FailedDeleteManager getFailedDeleteManager()
    {
        return failedDeleteManager;
    }
}
