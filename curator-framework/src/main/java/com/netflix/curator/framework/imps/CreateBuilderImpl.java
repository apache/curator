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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.netflix.curator.RetryLoop;
import com.netflix.curator.TimeTrace;
import com.netflix.curator.framework.api.*;
import com.netflix.curator.framework.api.transaction.CuratorTransactionBridge;
import com.netflix.curator.framework.api.transaction.OperationType;
import com.netflix.curator.framework.api.transaction.TransactionCreateBuilder;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.data.ACL;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

class CreateBuilderImpl implements CreateBuilder, BackgroundOperation<PathAndBytes>
{
    private final CuratorFrameworkImpl      client;
    private CreateMode                      createMode;
    private Backgrounding                   backgrounding;
    private boolean                         createParentsIfNeeded;
    private boolean                         doProtected;
    private boolean                         compress;
    private String                          protectedId;
    private ACLing                          acling;

    @VisibleForTesting
    boolean failNextCreateForTesting = false;

    @VisibleForTesting
    static final String         PROTECTED_PREFIX = "_c_";

    CreateBuilderImpl(CuratorFrameworkImpl client)
    {
        this.client = client;
        createMode = CreateMode.PERSISTENT;
        backgrounding = new Backgrounding();
        acling = new ACLing(client.getAclProvider());
        createParentsIfNeeded = false;
        compress = false;
        doProtected = false;
        protectedId = null;
    }

    TransactionCreateBuilder        asTransactionCreateBuilder(final CuratorTransactionImpl curatorTransaction, final CuratorMultiTransactionRecord transaction)
    {
        return new TransactionCreateBuilder()
        {
            @Override
            public PathAndBytesable<CuratorTransactionBridge> withACL(List<ACL> aclList)
            {
                CreateBuilderImpl.this.withACL(aclList);
                return this;
            }

            @Override
            public ACLPathAndBytesable<CuratorTransactionBridge> withMode(CreateMode mode)
            {
                CreateBuilderImpl.this.withMode(mode);
                return this;
            }

            @Override
            public CuratorTransactionBridge forPath(String path) throws Exception
            {
                return forPath(path, client.getDefaultData());
            }

            @Override
            public CuratorTransactionBridge forPath(String path, byte[] data) throws Exception
            {
                String      fixedPath = client.fixForNamespace(path);
                transaction.add(Op.create(fixedPath, data, acling.getAclList(path), createMode), OperationType.CREATE, path);
                return curatorTransaction;
            }
        };
    }

    @Override
    public CreateBackgroundModeACLable compressed()
    {
        compress = true;
        return new CreateBackgroundModeACLable()
        {
            @Override
            public ACLCreateModePathAndBytesable<String> creatingParentsIfNeeded()
            {
                createParentsIfNeeded = true;
                return asACLCreateModePathAndBytesable();
            }

            @Override
            public ACLPathAndBytesable<String> withProtectedEphemeralSequential()
            {
                return CreateBuilderImpl.this.withProtectedEphemeralSequential();
            }

            @Override
            public BackgroundPathAndBytesable<String> withACL(List<ACL> aclList)
            {
                return CreateBuilderImpl.this.withACL(aclList);
            }

            @Override
            public PathAndBytesable<String> inBackground(BackgroundCallback callback, Object context)
            {
                return CreateBuilderImpl.this.inBackground(callback, context);
            }

            @Override
            public PathAndBytesable<String> inBackground(BackgroundCallback callback, Object context, Executor executor)
            {
                return CreateBuilderImpl.this.inBackground(callback, context, executor);
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
            public ACLBackgroundPathAndBytesable<String> withMode(CreateMode mode)
            {
                return CreateBuilderImpl.this.withMode(mode);
            }

            @Override
            public String forPath(String path, byte[] data) throws Exception
            {
                return CreateBuilderImpl.this.forPath(path, data);
            }

            @Override
            public String forPath(String path) throws Exception
            {
                return CreateBuilderImpl.this.forPath(path);
            }
        };
    }

    @Override
    public ACLBackgroundPathAndBytesable<String> withACL(List<ACL> aclList)
    {
        acling = new ACLing(client.getAclProvider(), aclList);
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
            public PathAndBytesable<String> inBackground(BackgroundCallback callback, Object context)
            {
                return CreateBuilderImpl.this.inBackground(callback, context);
            }

            @Override
            public PathAndBytesable<String> inBackground(BackgroundCallback callback, Object context, Executor executor)
            {
                return CreateBuilderImpl.this.inBackground(callback, context, executor);
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

            @Override
            public String forPath(String path) throws Exception
            {
                return CreateBuilderImpl.this.forPath(path);
            }
        };
    }

    @Override
    public ProtectACLCreateModePathAndBytesable<String> creatingParentsIfNeeded()
    {
        createParentsIfNeeded = true;
        return new ProtectACLCreateModePathAndBytesable<String>()
        {
            @Override
            public ACLCreateModeBackgroundPathAndBytesable<String> withProtection()
            {
                return CreateBuilderImpl.this.withProtection();
            }

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
            public PathAndBytesable<String> inBackground(BackgroundCallback callback, Object context)
            {
                return CreateBuilderImpl.this.inBackground(callback, context);
            }

            @Override
            public PathAndBytesable<String> inBackground(BackgroundCallback callback, Executor executor)
            {
                return CreateBuilderImpl.this.inBackground(callback, executor);
            }

            @Override
            public PathAndBytesable<String> inBackground(BackgroundCallback callback, Object context, Executor executor)
            {
                return CreateBuilderImpl.this.inBackground(callback, context, executor);
            }

            @Override
            public ACLBackgroundPathAndBytesable<String> withMode(CreateMode mode)
            {
                return CreateBuilderImpl.this.withMode(mode);
            }

            @Override
            public String forPath(String path, byte[] data) throws Exception
            {
                return CreateBuilderImpl.this.forPath(path, data);
            }

            @Override
            public String forPath(String path) throws Exception
            {
                return CreateBuilderImpl.this.forPath(path);
            }
        };
    }

    @Override
    public ACLCreateModeBackgroundPathAndBytesable<String> withProtection()
    {
        setProtected();
        return this;
    }

    @Override
    public ACLPathAndBytesable<String> withProtectedEphemeralSequential()
    {
        setProtected();
        createMode = CreateMode.EPHEMERAL_SEQUENTIAL;

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

            @Override
            public String forPath(String path) throws Exception
            {
                return CreateBuilderImpl.this.forPath(path);
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
    public PathAndBytesable<String> inBackground(BackgroundCallback callback, Object context)
    {
        backgrounding = new Backgrounding(callback, context);
        return this;
    }

    @Override
    public PathAndBytesable<String> inBackground(BackgroundCallback callback, Object context, Executor executor)
    {
        backgrounding = new Backgrounding(client, callback, context, executor);
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
    public String forPath(String path) throws Exception
    {
        return forPath(path, client.getDefaultData());
    }

    @Override
    public String forPath(final String givenPath, byte[] data) throws Exception
    {
        if ( compress )
        {
            data = client.getCompressionProvider().compress(givenPath, data);
        }

        final String    adjustedPath = adjustPath(client.fixForNamespace(givenPath));

        String  returnPath = null;
        if ( backgrounding.inBackground() )
        {
            pathInBackground(adjustedPath, data, givenPath);
        }
        else
        {
            returnPath = pathInForeground(adjustedPath, data);
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
            acling.getAclList(operationAndData.getData().getPath()),
            createMode,
            new AsyncCallback.StringCallback()
            {
                @Override
                public void processResult(int rc, String path, Object ctx, String name)
                {
                    trace.commit();

                    if ( (rc == KeeperException.Code.NONODE.intValue()) && createParentsIfNeeded )
                    {
                        backgroundCreateParentsThenNode(operationAndData);
                    }
                    else
                    {
                        sendBackgroundResponse(rc, path, ctx, name, operationAndData);
                    }
                }
            },
            backgrounding.getContext()
        );
    }

    private String getProtectedPrefix()
    {
        return PROTECTED_PREFIX + protectedId + "-";
    }

    private void backgroundCreateParentsThenNode(final OperationAndData<PathAndBytes> mainOperationAndData)
    {
        BackgroundOperation<PathAndBytes>     operation = new BackgroundOperation<PathAndBytes>()
        {
            @Override
            public void performBackgroundOperation(OperationAndData<PathAndBytes> dummy) throws Exception
            {
                try
                {
                    ZKPaths.mkdirs(client.getZooKeeper(), mainOperationAndData.getData().getPath(), false);
                }
                catch ( KeeperException e )
                {
                    // ignore
                }
                client.queueOperation(mainOperationAndData);
            }
        };
        OperationAndData<PathAndBytes>        parentOperation = new OperationAndData<PathAndBytes>(operation, mainOperationAndData.getData(), null, null);
        client.queueOperation(parentOperation);
    }

    private void sendBackgroundResponse(int rc, String path, Object ctx, String name, OperationAndData<PathAndBytes> operationAndData)
    {
        path = client.unfixForNamespace(path);
        name = client.unfixForNamespace(name);

        CuratorEvent event = new CuratorEventImpl(client, CuratorEventType.CREATE, rc, path, name, ctx, null, null, null, null, null);
        client.processBackgroundOperation(operationAndData, event);
    }

    private void setProtected()
    {
        doProtected = true;
        protectedId = UUID.randomUUID().toString();
    }

    private ACLCreateModePathAndBytesable<String> asACLCreateModePathAndBytesable()
    {
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

                    @Override
                    public String forPath(String path) throws Exception
                    {
                        return CreateBuilderImpl.this.forPath(path);
                    }
                };
            }

            @Override
            public String forPath(String path, byte[] data) throws Exception
            {
                return CreateBuilderImpl.this.forPath(path, data);
            }

            @Override
            public String forPath(String path) throws Exception
            {
                return CreateBuilderImpl.this.forPath(path);
            }
        };
    }

    private void pathInBackground(final String path, final byte[] data, final String givenPath)
    {
        final AtomicBoolean firstTime = new AtomicBoolean(true);
        OperationAndData<PathAndBytes> operationAndData = new OperationAndData<PathAndBytes>(this, new PathAndBytes(path, data), backgrounding.getCallback(), null)
        {
            @Override
            void callPerformBackgroundOperation() throws Exception
            {
                boolean   callSuper = true;
                boolean   localFirstTime = firstTime.getAndSet(false);
                if ( !localFirstTime && doProtected )
                {
                    String      createdPath = findProtectedNodeInForeground(path);
                    if ( createdPath != null )
                    {
                        try
                        {
                            sendBackgroundResponse(KeeperException.Code.OK.intValue(), createdPath, backgrounding.getContext(), ZKPaths.getNodeFromPath(createdPath), this);
                        }
                        catch ( Exception e )
                        {
                            client.logError("Processing protected create for path: " + givenPath, e);
                        }
                        callSuper = false;
                    }
                }

                if ( failNextCreateForTesting )
                {
                    pathInForeground(path, data);   // simulate success on server without notification to client
                    failNextCreateForTesting = false;
                    throw new KeeperException.ConnectionLossException();
                }

                if ( callSuper )
                {
                    super.callPerformBackgroundOperation();
                }
            }
        };
        client.processBackgroundOperation(operationAndData, null);
    }

    private String pathInForeground(final String path, final byte[] data) throws Exception
    {
        TimeTrace               trace = client.getZookeeperClient().startTracer("CreateBuilderImpl-Foreground");

        final AtomicBoolean     firstTime = new AtomicBoolean(true);
        String                  returnPath = RetryLoop.callWithRetry
        (
            client.getZookeeperClient(),
            new Callable<String>()
            {
                @Override
                public String call() throws Exception
                {
                    boolean   localFirstTime = firstTime.getAndSet(false);

                    String    createdPath = null;
                    if ( !localFirstTime && doProtected )
                    {
                        createdPath = findProtectedNodeInForeground(path);
                    }

                    if ( createdPath == null )
                    {
                        try
                        {
                            createdPath = client.getZooKeeper().create(path, data, acling.getAclList(path), createMode);
                        }
                        catch ( KeeperException.NoNodeException e )
                        {
                            if ( createParentsIfNeeded )
                            {
                                ZKPaths.mkdirs(client.getZooKeeper(), path, false);
                                createdPath = client.getZooKeeper().create(path, data, acling.getAclList(path), createMode);
                            }
                            else
                            {
                                throw e;
                            }
                        }
                    }

                    if ( failNextCreateForTesting )
                    {
                        failNextCreateForTesting = false;
                        throw new KeeperException.ConnectionLossException();
                    }
                    return createdPath;
                }
            }
        );

        trace.commit();
        return returnPath;
    }

    private String  findProtectedNodeInForeground(final String path) throws Exception
    {
        TimeTrace       trace = client.getZookeeperClient().startTracer("CreateBuilderImpl-findProtectedNodeInForeground");

        String          returnPath = RetryLoop.callWithRetry
        (
            client.getZookeeperClient(),
            new Callable<String>()
            {
                @Override
                public String call() throws Exception
                {
                    String foundNode = null;
                    try
                    {
                        final ZKPaths.PathAndNode   pathAndNode = ZKPaths.getPathAndNode(path);
                        List<String>                children = client.getZooKeeper().getChildren(pathAndNode.getPath(), false);

                        final String                protectedPrefix = getProtectedPrefix();
                        foundNode = Iterables.find
                        (
                            children,
                            new Predicate<String>()
                            {
                                @Override
                                public boolean apply(String node)
                                {
                                    return node.startsWith(protectedPrefix);
                                }
                            },
                            null
                        );
                        if ( foundNode != null )
                        {
                            foundNode = ZKPaths.makePath(pathAndNode.getPath(), foundNode);
                        }
                    }
                    catch ( KeeperException.NoNodeException ignore )
                    {
                        // ignore
                    }
                    return foundNode;
                }
            }
        );

        trace.commit();
        return returnPath;
    }

    private String  adjustPath(String path) throws Exception
    {
        if ( doProtected )
        {
            ZKPaths.PathAndNode     pathAndNode = ZKPaths.getPathAndNode(path);
            String                  name = getProtectedPrefix() + pathAndNode.getNode();
            path = ZKPaths.makePath(pathAndNode.getPath(), name);
        }
        return path;
    }
}
