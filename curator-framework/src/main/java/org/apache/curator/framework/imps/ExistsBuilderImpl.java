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

import org.apache.curator.RetryLoop;
import org.apache.curator.drivers.OperationTrace;
import org.apache.curator.framework.api.*;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

public class ExistsBuilderImpl implements ExistsBuilder, BackgroundOperation<String>, ErrorListenerPathable<Stat>, ACLableExistBuilderMain
{
    private final CuratorFrameworkImpl client;
    private Backgrounding backgrounding;
    private Watching watching;
    private boolean createParentsIfNeeded;
    private boolean createParentContainersIfNeeded;
    private ACLing acling;

    ExistsBuilderImpl(CuratorFrameworkImpl client)
    {
        this(client, new Backgrounding(), null, false, false);
    }

    public ExistsBuilderImpl(CuratorFrameworkImpl client, Backgrounding backgrounding, Watcher watcher, boolean createParentsIfNeeded, boolean createParentContainersIfNeeded)
    {
        this.client = client;
        this.backgrounding = backgrounding;
        this.watching = new Watching(client, watcher);
        this.createParentsIfNeeded = createParentsIfNeeded;
        this.createParentContainersIfNeeded = createParentContainersIfNeeded;
        this.acling = new ACLing(client.getAclProvider());
    }

    @Override
    public ACLableExistBuilderMain creatingParentsIfNeeded()
    {
        createParentContainersIfNeeded = false;
        createParentsIfNeeded = true;
        return this;
    }

    @Override
    public ACLableExistBuilderMain creatingParentContainersIfNeeded()
    {
        createParentContainersIfNeeded = true;
        createParentsIfNeeded = false;
        return this;
    }

    @Override
    public ExistsBuilderMain withACL(List<ACL> aclList)
    {
        acling = new ACLing(client.getAclProvider(), aclList, true);
        return this;
    }

    @Override
    public BackgroundPathable<Stat> watched()
    {
        watching = new Watching(client, true);
        return this;
    }

    @Override
    public BackgroundPathable<Stat> usingWatcher(Watcher watcher)
    {
        watching = new Watching(client, watcher);
        return this;
    }

    @Override
    public BackgroundPathable<Stat> usingWatcher(CuratorWatcher watcher)
    {
        watching = new Watching(client, watcher);
        return this;
    }

    @Override
    public ErrorListenerPathable<Stat> inBackground(BackgroundCallback callback, Object context)
    {
        backgrounding = new Backgrounding(callback, context);
        return this;
    }

    @Override
    public ErrorListenerPathable<Stat> inBackground(BackgroundCallback callback, Object context, Executor executor)
    {
        backgrounding = new Backgrounding(client, callback, context, executor);
        return this;
    }

    @Override
    public ErrorListenerPathable<Stat> inBackground(BackgroundCallback callback)
    {
        backgrounding = new Backgrounding(callback);
        return this;
    }

    @Override
    public ErrorListenerPathable<Stat> inBackground(BackgroundCallback callback, Executor executor)
    {
        backgrounding = new Backgrounding(client, callback, executor);
        return this;
    }

    @Override
    public ErrorListenerPathable<Stat> inBackground()
    {
        backgrounding = new Backgrounding(true);
        return this;
    }

    @Override
    public ErrorListenerPathable<Stat> inBackground(Object context)
    {
        backgrounding = new Backgrounding(context);
        return this;
    }

    @Override
    public Pathable<Stat> withUnhandledErrorListener(UnhandledErrorListener listener)
    {
        backgrounding = new Backgrounding(backgrounding, listener);
        return this;
    }

    @Override
    public void performBackgroundOperation(final OperationAndData<String> operationAndData) throws Exception
    {
        try
        {
            final OperationTrace   trace = client.getZookeeperClient().startAdvancedTracer("ExistsBuilderImpl-Background");
            AsyncCallback.StatCallback callback = new AsyncCallback.StatCallback()
            {
                @Override
                public void processResult(int rc, String path, Object ctx, Stat stat)
                {
                    watching.commitWatcher(rc, true);
                    trace.setReturnCode(rc).setPath(path).setWithWatcher(watching.hasWatcher()).setStat(stat).commit();
                    CuratorEvent event = new CuratorEventImpl(client, CuratorEventType.EXISTS, rc, path, null, ctx, stat, null, null, null, null, null);
                    client.processBackgroundOperation(operationAndData, event);
                }
            };
            if ( watching.isWatched() )
            {
                client.getZooKeeper().exists(operationAndData.getData(), true, callback, backgrounding.getContext());
            }
            else
            {
                client.getZooKeeper().exists(operationAndData.getData(), watching.getWatcher(operationAndData.getData()), callback, backgrounding.getContext());
            }
        }
        catch ( Throwable e )
        {
            backgrounding.checkError(e, watching);
        }
    }

    @Override
    public Stat forPath(String path) throws Exception
    {
        path = client.fixForNamespace(path);

        client.getSchemaSet().getSchema(path).validateWatch(path, watching.isWatched() || watching.hasWatcher());

        Stat        returnStat = null;
        if ( backgrounding.inBackground() )
        {
            OperationAndData<String> operationAndData = new OperationAndData<String>(this, path, backgrounding.getCallback(), null, backgrounding.getContext(), watching);
            if ( createParentContainersIfNeeded || createParentsIfNeeded )
            {
                CreateBuilderImpl.backgroundCreateParentsThenNode(client, operationAndData, operationAndData.getData(), backgrounding, acling.getACLProviderForParents(), createParentContainersIfNeeded);
            }
            else
            {
                client.processBackgroundOperation(operationAndData, null);
            }
        }
        else
        {
            returnStat = pathInForeground(path);
        }

        return returnStat;
    }

    private Stat pathInForeground(final String path) throws Exception
    {
        if ( createParentContainersIfNeeded || createParentsIfNeeded )
        {
            final String parent = ZKPaths.getPathAndNode(path).getPath();
            if ( !parent.equals(ZKPaths.PATH_SEPARATOR) )
            {
                OperationTrace   trace = client.getZookeeperClient().startAdvancedTracer("ExistsBuilderImpl-Foreground-CreateParents");
                RetryLoop.callWithRetry
                (
                    client.getZookeeperClient(),
                    new Callable<Void>()
                    {
                        @Override
                        public Void call() throws Exception
                        {
                            try
                            {
                                ZKPaths.mkdirs(client.getZooKeeper(), parent, true, acling.getACLProviderForParents(), createParentContainersIfNeeded);
                            }
                            catch ( KeeperException.NodeExistsException e )
                            {
                                // ignore
                            }
                            catch ( KeeperException.NoNodeException e )
                            {
                                // ignore
                            }
                            return null;
                        }
                    }
                );
                trace.setPath(path).commit();
            }
        }
        return pathInForegroundStandard(path);
    }

    private Stat pathInForegroundStandard(final String path) throws Exception
    {
        OperationTrace   trace = client.getZookeeperClient().startAdvancedTracer("ExistsBuilderImpl-Foreground");
        Stat        returnStat = RetryLoop.callWithRetry
        (
            client.getZookeeperClient(),
            new Callable<Stat>()
            {
                @Override
                public Stat call() throws Exception
                {
                    Stat    returnStat;
                    if ( watching.isWatched() )
                    {
                        returnStat = client.getZooKeeper().exists(path, true);
                    }
                    else
                    {
                        returnStat = client.getZooKeeper().exists(path, watching.getWatcher(path));
                        int rc = (returnStat != null) ? KeeperException.NoNodeException.Code.OK.intValue() : KeeperException.NoNodeException.Code.NONODE.intValue();
                        watching.commitWatcher(rc, true);
                    }
                    return returnStat;
                }
            }
        );
        trace.setPath(path).setWithWatcher(watching.hasWatcher()).setStat(returnStat).commit();
        return returnStat;
    }
}
