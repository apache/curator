/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.curator.framework.imps;

import com.netflix.curator.CuratorZookeeperClient;
import com.netflix.curator.RetryLoop;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.*;
import com.netflix.curator.framework.listen.Listenable;
import com.netflix.curator.framework.state.ConnectionStateListener;
import com.netflix.curator.utils.EnsurePath;
import org.apache.zookeeper.ZooKeeper;

class NamespaceFacade extends CuratorFrameworkImpl
{
    private final CuratorFrameworkImpl client;
    private final NamespaceImpl namespace;

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
    public CreateBuilder create()
    {
        return new CreateBuilderImpl(this);
    }

    @Override
    public DeleteBuilder delete()
    {
        return new DeleteBuilderImpl(this);
    }

    @Override
    public ExistsBuilder checkExists()
    {
        return new ExistsBuilderImpl(this);
    }

    @Override
    public GetDataBuilder getData()
    {
        return new GetDataBuilderImpl(this);
    }

    @Override
    public SetDataBuilder setData()
    {
        return new SetDataBuilderImpl(this);
    }

    @Override
    public GetChildrenBuilder getChildren()
    {
        return new GetChildrenBuilderImpl(this);
    }

    @Override
    public boolean isStarted()
    {
        return client.isStarted();
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
}
