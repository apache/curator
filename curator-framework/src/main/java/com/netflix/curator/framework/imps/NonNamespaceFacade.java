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

import com.netflix.curator.RetryLoop;
import com.netflix.curator.CuratorZookeeperClient;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.CreateBuilder;
import com.netflix.curator.framework.api.ExistsBuilder;
import com.netflix.curator.framework.api.GetChildrenBuilder;
import com.netflix.curator.framework.api.GetDataBuilder;
import com.netflix.curator.framework.api.CuratorListener;
import com.netflix.curator.framework.api.DeleteBuilder;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.SetDataBuilder;
import com.netflix.curator.utils.EnsurePath;
import org.apache.zookeeper.ZooKeeper;
import java.util.concurrent.Executor;

public class NonNamespaceFacade extends CuratorFrameworkImpl
{
    private final CuratorFrameworkImpl client;

    NonNamespaceFacade(CuratorFrameworkImpl client)
    {
        super(client);
        this.client = client;
    }

    @Override
    public CuratorFramework nonNamespaceView()
    {
        return this;
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
    public void addListener(CuratorListener listener)
    {
        client.addListener(listener);
    }

    @Override
    public void addListener(CuratorListener listener, Executor executor)
    {
        client.addListener(listener, executor);
    }

    @Override
    public void removeListener(CuratorListener listener)
    {
        client.removeListener(listener);
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
    void notifyErrorClosing(int resultCode, Throwable e)
    {
        client.notifyErrorClosing(resultCode, e);
    }

    @Override
    String unfixForNamespace(String path)
    {
        return path;
    }

    @Override
    String fixForNamespace(String path)
    {
        return path;
    }

    @Override
    public EnsurePath newNamespaceAwareEnsurePath(String path)
    {
        return new EnsurePath(path);    // no namespace
    }
}
