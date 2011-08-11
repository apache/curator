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
import com.netflix.curator.TimeTrace;
import com.netflix.curator.framework.api.*;
import com.netflix.curator.framework.api.CuratorEventType;
import com.netflix.curator.framework.api.CreateBuilder;
import com.netflix.curator.framework.api.BackgroundCallback;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

class CreateBuilderImpl implements CreateBuilder, BackgroundOperation<PathAndBytes>
{
    private final CuratorFrameworkImpl client;
    private CreateMode                  createMode;
    private Backgrounding               backgrounding;
    private boolean                     createParentsIfNeeded;
    private ACLing                      acling;

    CreateBuilderImpl(CuratorFrameworkImpl client)
    {
        this.client = client;
        createMode = CreateMode.PERSISTENT;
        backgrounding = new Backgrounding();
        acling = new ACLing();
        createParentsIfNeeded = false;
    }

    @Override
    public ACLBackgroundPathAndBytesable<String> withACL(List<ACL> aclList)
    {
        acling = new ACLing(aclList);
        return new ACLBackgroundPathAndBytesable<String>()
        {
            @Override
            public BackgroundPathAndBytesable<String> withACL(List<ACL> aclList)
            {
                return CreateBuilderImpl.this.withACL(aclList);
            }

            @Override
            public PathAndBytesable<String> inBackground()
            {
                return CreateBuilderImpl.this.inBackground();
            }

            @Override
            public PathAndBytesable<String> inBackground(Object context)
            {
                return CreateBuilderImpl.this.inBackground(context);
            }

            @Override
            public PathAndBytesable<String> inBackground(BackgroundCallback callback)
            {
                return CreateBuilderImpl.this.inBackground(callback);
            }

            @Override
            public PathAndBytesable<String> inBackground(BackgroundCallback callback, Executor executor)
            {
                return CreateBuilderImpl.this.inBackground(callback, executor);
            }

            @Override
            public String forPath(String path, byte[] data) throws Exception
            {
                return CreateBuilderImpl.this.forPath(path, data);
            }
        };
    }

    @Override
    public ACLCreateModePathAndBytesable<String> creatingParentsIfNeeded()
    {
        createParentsIfNeeded = true;
        return new ACLCreateModePathAndBytesable<String>()
        {
            @Override
            public PathAndBytesable<String> withACL(List<ACL> aclList)
            {
                return CreateBuilderImpl.this.withACL(aclList);
            }

            @Override
            public ACLPathAndBytesable<String> withMode(CreateMode mode)
            {
                createMode = mode;
                return new ACLPathAndBytesable<String>()
                {
                    @Override
                    public PathAndBytesable<String> withACL(List<ACL> aclList)
                    {
                        return CreateBuilderImpl.this.withACL(aclList);
                    }

                    @Override
                    public String forPath(String path, byte[] data) throws Exception
                    {
                        return CreateBuilderImpl.this.forPath(path, data);
                    }
                };
            }

            @Override
            public String forPath(String path, byte[] data) throws Exception
            {
                return CreateBuilderImpl.this.forPath(path, data);
            }
        };
    }

    @Override
    public ACLBackgroundPathAndBytesable<String> withMode(CreateMode mode)
    {
        createMode = mode;
        return this;
    }

    @Override
    public PathAndBytesable<String> inBackground(BackgroundCallback callback)
    {
        backgrounding = new Backgrounding(callback);
        return this;
    }

    @Override
    public PathAndBytesable<String> inBackground(BackgroundCallback callback, Executor executor)
    {
        backgrounding = new Backgrounding(client, callback, executor);
        return this;
    }

    @Override
    public PathAndBytesable<String> inBackground()
    {
        backgrounding = new Backgrounding(true);
        return this;
    }

    @Override
    public PathAndBytesable<String> inBackground(Object context)
    {
        backgrounding = new Backgrounding(context);
        return this;
    }

    @Override
    public String forPath(String path, byte[] data) throws Exception
    {
        path = client.fixForNamespace(path);

        String  returnPath = null;
        if ( backgrounding.inBackground() )
        {
            client.processBackgroundOperation(new OperationAndData<PathAndBytes>(this, new PathAndBytes(path, data), backgrounding.getCallback()), null);
        }
        else
        {
            returnPath = pathInForeground(path, data);
            returnPath = client.unfixForNamespace(returnPath);
        }
        return returnPath;
    }

    @Override
    public void performBackgroundOperation(final OperationAndData<PathAndBytes> operationAndData) throws Exception
    {
        final TimeTrace   trace = client.getZookeeperClient().startTracer("CreateBuilderImpl-Background");
        client.getZooKeeper().create
        (
            operationAndData.getData().getPath(),
            operationAndData.getData().getData(),
            acling.getAclList(),
            createMode,
            new AsyncCallback.StringCallback()
            {
                @Override
                public void processResult(int rc, String path, Object ctx, String name)
                {
                    path = client.unfixForNamespace(path);
                    name = client.unfixForNamespace(name);

                    trace.commit();
                    CuratorEvent event = new CuratorEventImpl(CuratorEventType.CREATE, rc, path, name, ctx, null, null, null, null, null);
                    client.processBackgroundOperation(operationAndData, event);
                }
            },
            backgrounding.getContext()
        );
    }

    private String pathInForeground(final String path, final byte[] data) throws Exception
    {
        TimeTrace trace = client.getZookeeperClient().startTracer("CreateBuilderImpl-Foreground");
        String    returnPath = RetryLoop.callWithRetry
        (
            client.getZookeeperClient(),
            new Callable<String>()
            {
                @Override
                public String call() throws Exception
                {
                    if ( createParentsIfNeeded )
                    {
                        ZKPaths.mkdirs(client.getZooKeeper(), path, false);
                    }
                    return client.getZooKeeper().create(path, data, acling.getAclList(), createMode);
                }
            }
        );
        trace.commit();
        return returnPath;
    }
}
