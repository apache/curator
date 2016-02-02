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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.apache.curator.RetryLoop;
import org.apache.curator.TimeTrace;
import org.apache.curator.framework.api.*;
import org.apache.curator.framework.api.transaction.OperationType;
import org.apache.curator.framework.api.transaction.TransactionCreateBuilder;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

class CreateBuilderImpl implements CreateBuilder, BackgroundOperation<PathAndBytes>
{
    private final CuratorFrameworkImpl client;
    private CreateMode createMode;
    private Backgrounding backgrounding;
    private boolean createParentsIfNeeded;
    private boolean createParentsAsContainers;
    private boolean doProtected;
    private boolean compress;
    private boolean setDataIfExists;
    private String protectedId;
    private ACLing acling;
    private Stat storingStat;

    @VisibleForTesting
    boolean failNextCreateForTesting = false;

    @VisibleForTesting
    static final String PROTECTED_PREFIX = "_c_";

    CreateBuilderImpl(CuratorFrameworkImpl client)
    {
        this.client = client;
        createMode = CreateMode.PERSISTENT;
        backgrounding = new Backgrounding();
        acling = new ACLing(client.getAclProvider());
        createParentsIfNeeded = false;
        createParentsAsContainers = false;
        compress = false;
        doProtected = false;
        setDataIfExists = false;
        protectedId = null;
        storingStat = null;
    }

    @Override
    public CreateBuilderMain orSetData()
    {
        setDataIfExists = true;
        return this;
    }

    <T> TransactionCreateBuilder<T> asTransactionCreateBuilder(final T context, final CuratorMultiTransactionRecord transaction)
    {
        return new TransactionCreateBuilder<T>()
        {
            @Override
            public PathAndBytesable<T> withACL(List<ACL> aclList)
            {
                CreateBuilderImpl.this.withACL(aclList);
                return this;
            }

            @Override
            public ACLPathAndBytesable<T> withMode(CreateMode mode)
            {
                CreateBuilderImpl.this.withMode(mode);
                return this;
            }

            @Override
            public ACLCreateModePathAndBytesable<T> compressed()
            {
                CreateBuilderImpl.this.compressed();
                return this;
            }

            @Override
            public T forPath(String path) throws Exception
            {
                return forPath(path, client.getDefaultData());
            }

            @Override
            public T forPath(String path, byte[] data) throws Exception
            {
                if ( compress )
                {
                    data = client.getCompressionProvider().compress(path, data);
                }

                String fixedPath = client.fixForNamespace(path);
                transaction.add(Op.create(fixedPath, data, acling.getAclList(path), createMode), OperationType.CREATE, path);
                return context;
            }
        };
    }

    @Override
    public CreateBackgroundModeStatACLable compressed()
    {
        compress = true;
        return new CreateBackgroundModeStatACLable()
        {
            @Override
            public CreateBackgroundModeACLable storingStatIn(Stat stat) {
                storingStat = stat;
                return asCreateBackgroundModeACLable();
            }
            
            @Override
            public ACLCreateModePathAndBytesable<String> creatingParentsIfNeeded()
            {
                createParentsIfNeeded = true;
                return asACLCreateModePathAndBytesable();
            }

            @Override
            public ACLCreateModePathAndBytesable<String> creatingParentContainersIfNeeded()
            {
                setCreateParentsAsContainers();
                return creatingParentsIfNeeded();
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
    public ProtectACLCreateModeStatPathAndBytesable<String> creatingParentContainersIfNeeded()
    {
        setCreateParentsAsContainers();
        return creatingParentsIfNeeded();
    }

    private void setCreateParentsAsContainers()
    {
        if ( client.useContainerParentsIfAvailable() )
        {
            createParentsAsContainers = true;
        }
    }

    @Override
    public ProtectACLCreateModeStatPathAndBytesable<String> creatingParentsIfNeeded()
    {
        createParentsIfNeeded = true;
        return new ProtectACLCreateModeStatPathAndBytesable<String>()
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

            @Override
            public ACLBackgroundPathAndBytesable<String> storingStatIn(Stat stat) {
                storingStat = stat;
                return CreateBuilderImpl.this;
            }
        };
    }

    @Override
    public ACLCreateModeStatBackgroundPathAndBytesable<String> withProtection()
    {
        setProtected();
        return asACLCreateModeStatBackgroundPathAndBytesable();
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

        final String adjustedPath = adjustPath(client.fixForNamespace(givenPath, createMode.isSequential()));

        String returnPath = null;
        if ( backgrounding.inBackground() )
        {
            pathInBackground(adjustedPath, data, givenPath);
        }
        else
        {
            String path = protectedPathInForeground(adjustedPath, data);
            returnPath = client.unfixForNamespace(path);
        }
        return returnPath;
    }

    private String protectedPathInForeground(String adjustedPath, byte[] data) throws Exception
    {
        try
        {
            return pathInForeground(adjustedPath, data);
        }
        catch ( Exception e)
        {
            ThreadUtils.checkInterrupted(e);
            if ( ( e instanceof KeeperException.ConnectionLossException ||
                !( e instanceof KeeperException )) && protectedId != null )
            {
                /*
                 * CURATOR-45 + CURATOR-79: we don't know if the create operation was successful or not,
                 * register the znode to be sure it is deleted later.
                 */
                new FindAndDeleteProtectedNodeInBackground(client, ZKPaths.getPathAndNode(adjustedPath).getPath(), protectedId).execute();
                /*
                * The current UUID is scheduled to be deleted, it is not safe to use it again.
                * If this builder is used again later create a new UUID
                */
                protectedId = UUID.randomUUID().toString();
            }

            throw e;
        }
    }

    @Override
    public void performBackgroundOperation(final OperationAndData<PathAndBytes> operationAndData) throws Exception
    {
        final TimeTrace trace = client.getZookeeperClient().startTracer("CreateBuilderImpl-Background");
        
        if(storingStat == null)
        {
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
                            backgroundCreateParentsThenNode(client, operationAndData, operationAndData.getData().getPath(), backgrounding, createParentsAsContainers);
                        }
                        else if ( (rc == KeeperException.Code.NODEEXISTS.intValue()) && setDataIfExists )
                        {
                            backgroundSetData(client, operationAndData, operationAndData.getData().getPath(), backgrounding);
                        }
                        else
                        {
                            sendBackgroundResponse(rc, path, ctx, name, null, operationAndData);
                        }
                    }
                },
                backgrounding.getContext()
            );
        }
        else
        {
            client.getZooKeeper().create
            (
                operationAndData.getData().getPath(),
                operationAndData.getData().getData(),
                acling.getAclList(operationAndData.getData().getPath()),
                createMode,
                new AsyncCallback.Create2Callback() {
                    
                    @Override
                    public void processResult(int rc, String path, Object ctx, String name, Stat stat) {
                        trace.commit();

                        if ( stat != null )
                        {
                            storingStat.setAversion(stat.getAversion());
                            storingStat.setCtime(stat.getCtime());
                            storingStat.setCversion(stat.getCversion());
                            storingStat.setCzxid(stat.getCzxid());
                            storingStat.setDataLength(stat.getDataLength());
                            storingStat.setEphemeralOwner(stat.getEphemeralOwner());
                            storingStat.setMtime(stat.getMtime());
                            storingStat.setMzxid(stat.getMzxid());
                            storingStat.setNumChildren(stat.getNumChildren());
                            storingStat.setPzxid(stat.getPzxid());
                            storingStat.setVersion(stat.getVersion());
                        }

                        if ( (rc == KeeperException.Code.NONODE.intValue()) && createParentsIfNeeded )
                        {
                            backgroundCreateParentsThenNode(client, operationAndData, operationAndData.getData().getPath(), backgrounding, createParentsAsContainers);
                        }
                        else
                        {
                            sendBackgroundResponse(rc, path, ctx, name, stat, operationAndData);
                        }
                    }
                },
                backgrounding.getContext()
            );
        }
    }
    
    @Override
    public CreateProtectACLCreateModePathAndBytesable<String> storingStatIn(Stat stat) {
        storingStat = stat;
        
        return new CreateProtectACLCreateModePathAndBytesable<String>() {

            @Override
            public BackgroundPathAndBytesable<String> withACL(List<ACL> aclList) {
                return CreateBuilderImpl.this.withACL(aclList);
            }

            @Override
            public PathAndBytesable<String> inBackground() {
                return CreateBuilderImpl.this.inBackground();
            }

            @Override
            public PathAndBytesable<String> inBackground(Object context) {
                return CreateBuilderImpl.this.inBackground(context);
            }

            @Override
            public PathAndBytesable<String> inBackground(BackgroundCallback callback) {
                return CreateBuilderImpl.this.inBackground(callback);
            }

            @Override
            public PathAndBytesable<String> inBackground(BackgroundCallback callback, Object context) {
                return CreateBuilderImpl.this.inBackground(callback, context);
            }

            @Override
            public PathAndBytesable<String> inBackground(BackgroundCallback callback, Executor executor) {
                return CreateBuilderImpl.this.inBackground(callback, executor);
            }

            @Override
            public PathAndBytesable<String> inBackground(BackgroundCallback callback, Object context,
                    Executor executor) {
                return CreateBuilderImpl.this.inBackground(callback, context, executor);
            }

            @Override
            public String forPath(String path, byte[] data) throws Exception {
                return CreateBuilderImpl.this.forPath(path, data);
            }

            @Override
            public String forPath(String path) throws Exception {
                return CreateBuilderImpl.this.forPath(path);
            }

            @Override
            public ACLBackgroundPathAndBytesable<String> withMode(CreateMode mode) {
                return CreateBuilderImpl.this.withMode(mode);
            }

            @Override
            public ACLCreateModeBackgroundPathAndBytesable<String> withProtection() {
                return CreateBuilderImpl.this.withProtection();
            }

            @Override
            public ProtectACLCreateModePathAndBytesable<String> creatingParentsIfNeeded() {
                return CreateBuilderImpl.this.creatingParentsIfNeeded();
            }

            @Override
            public ProtectACLCreateModePathAndBytesable<String> creatingParentContainersIfNeeded() {
                return CreateBuilderImpl.this.creatingParentContainersIfNeeded();
            }  
        };
    }

    private static String getProtectedPrefix(String protectedId)
    {
        return PROTECTED_PREFIX + protectedId + "-";
    }

    static <T> void backgroundCreateParentsThenNode(final CuratorFrameworkImpl client, final OperationAndData<T> mainOperationAndData, final String path, Backgrounding backgrounding, final boolean createParentsAsContainers)
    {
        BackgroundOperation<T> operation = new BackgroundOperation<T>()
        {
            @Override
            public void performBackgroundOperation(OperationAndData<T> dummy) throws Exception
            {
                try
                {
                    ZKPaths.mkdirs(client.getZooKeeper(), path, false, client.getAclProvider(), createParentsAsContainers);
                }
                catch ( KeeperException e )
                {
                    // ignore
                }
                client.queueOperation(mainOperationAndData);
            }
        };
        OperationAndData<T> parentOperation = new OperationAndData<T>(operation, mainOperationAndData.getData(), null, null, backgrounding.getContext());
        client.queueOperation(parentOperation);
    }

    private void backgroundSetData(final CuratorFrameworkImpl client, final OperationAndData<PathAndBytes> mainOperationAndData, final String path, final Backgrounding backgrounding)
    {
        final AsyncCallback.StatCallback statCallback = new AsyncCallback.StatCallback()
        {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat)
            {
                if ( rc == KeeperException.Code.NONODE.intValue() )
                {
                    client.queueOperation(mainOperationAndData);    // try to create it again
                }
                else
                {
                    sendBackgroundResponse(rc, path, ctx, path, stat, mainOperationAndData);
                }
            }
        };
        BackgroundOperation<PathAndBytes> operation = new BackgroundOperation<PathAndBytes>()
        {
            @Override
            public void performBackgroundOperation(OperationAndData<PathAndBytes> op) throws Exception
            {
                try
                {
                    client.getZooKeeper().setData(path, mainOperationAndData.getData().getData(), -1, statCallback, backgrounding.getContext());
                }
                catch ( KeeperException e )
                {
                    // ignore
                }
                client.queueOperation(mainOperationAndData);
            }
        };
        client.queueOperation(new OperationAndData<>(operation, null, null, null, null));
    }

    private void sendBackgroundResponse(int rc, String path, Object ctx, String name, Stat stat, OperationAndData<PathAndBytes> operationAndData)
    {
        path = client.unfixForNamespace(path);
        name = client.unfixForNamespace(name);

        CuratorEvent event = new CuratorEventImpl(client, CuratorEventType.CREATE, rc, path, name, ctx, stat, null, null, null, null, null);
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
    
    private CreateBackgroundModeACLable asCreateBackgroundModeACLable()
    {
        return new CreateBackgroundModeACLable() {
            
            @Override
            public BackgroundPathAndBytesable<String> withACL(List<ACL> aclList) {
                return CreateBuilderImpl.this.withACL(aclList);
            }
            
            @Override
            public ACLBackgroundPathAndBytesable<String> withMode(CreateMode mode) {
                return CreateBuilderImpl.this.withMode(mode);
            }
            
            @Override
            public String forPath(String path) throws Exception {
                return CreateBuilderImpl.this.forPath(path);
            }
            
            @Override
            public String forPath(String path, byte[] data) throws Exception {
                return CreateBuilderImpl.this.forPath(path, data);
            }
            
            @Override
            public PathAndBytesable<String> inBackground(BackgroundCallback callback, Object context, Executor executor) {
                return CreateBuilderImpl.this.inBackground(callback, context, executor);
            }
            
            @Override
            public PathAndBytesable<String> inBackground(BackgroundCallback callback, Executor executor) {
                return CreateBuilderImpl.this.inBackground(callback, executor);
            }
            
            @Override
            public PathAndBytesable<String> inBackground(BackgroundCallback callback, Object context) {
                return CreateBuilderImpl.this.inBackground(callback, context);
            }
            
            @Override
            public PathAndBytesable<String> inBackground(BackgroundCallback callback) {
                return CreateBuilderImpl.this.inBackground(callback);
            }
            
            @Override
            public PathAndBytesable<String> inBackground(Object context) {
                return CreateBuilderImpl.this.inBackground(context);
            }
            
            @Override
            public PathAndBytesable<String> inBackground() {
                return CreateBuilderImpl.this.inBackground();
            }
            
            @Override
            public ACLPathAndBytesable<String> withProtectedEphemeralSequential() {
                return CreateBuilderImpl.this.withProtectedEphemeralSequential();
            }
            
            @Override
            public ACLCreateModePathAndBytesable<String> creatingParentsIfNeeded() {
                createParentsIfNeeded = true;
                return asACLCreateModePathAndBytesable();
            }
            
            @Override
            public ACLCreateModePathAndBytesable<String> creatingParentContainersIfNeeded() {
                setCreateParentsAsContainers();
                return asACLCreateModePathAndBytesable();
            }
        };
    }
    
    private ACLCreateModeStatBackgroundPathAndBytesable<String> asACLCreateModeStatBackgroundPathAndBytesable()
    {
        return new ACLCreateModeStatBackgroundPathAndBytesable<String>()
        {
            @Override
            public BackgroundPathAndBytesable<String> withACL(List<ACL> aclList) {
                return CreateBuilderImpl.this.withACL(aclList);
            }

            @Override
            public PathAndBytesable<String> inBackground() {
                return CreateBuilderImpl.this.inBackground();
            }

            @Override
            public PathAndBytesable<String> inBackground(BackgroundCallback callback, Object context, Executor executor) {
                return CreateBuilderImpl.this.inBackground(callback, context, executor);
            }
            
            @Override
            public PathAndBytesable<String> inBackground(BackgroundCallback callback, Executor executor) {
                return CreateBuilderImpl.this.inBackground(callback, executor);
            }
            
            @Override
            public PathAndBytesable<String> inBackground(BackgroundCallback callback, Object context) {
                return CreateBuilderImpl.this.inBackground(callback, context);
            }
            
            @Override
            public PathAndBytesable<String> inBackground(BackgroundCallback callback) {
                return CreateBuilderImpl.this.inBackground(callback);
            }
            
            @Override
            public PathAndBytesable<String> inBackground(Object context) {
                return CreateBuilderImpl.this.inBackground(context);
            }

            @Override
            public String forPath(String path) throws Exception {
                return CreateBuilderImpl.this.forPath(path);
            }
            
            @Override
            public String forPath(String path, byte[] data) throws Exception {
                return CreateBuilderImpl.this.forPath(path, data);
            }

            @Override
            public ACLBackgroundPathAndBytesable<String> withMode(CreateMode mode) {
                return CreateBuilderImpl.this.withMode(mode);
            }

            @Override
            public ACLCreateModeBackgroundPathAndBytesable<String> storingStatIn(Stat stat) {
                storingStat = stat;
                return CreateBuilderImpl.this;
            }            
        };
    }

    @VisibleForTesting
    volatile boolean debugForceFindProtectedNode = false;

    private void pathInBackground(final String path, final byte[] data, final String givenPath)
    {
        final AtomicBoolean firstTime = new AtomicBoolean(true);
        OperationAndData<PathAndBytes> operationAndData = new OperationAndData<PathAndBytes>(this, new PathAndBytes(path, data), backgrounding.getCallback(),
            new OperationAndData.ErrorCallback<PathAndBytes>()
            {
                public void retriesExhausted(OperationAndData<PathAndBytes> operationAndData)
                {
                    if ( doProtected )
                    {
                        // all retries have failed, findProtectedNodeInForeground(..) included, schedule a clean up
                        new FindAndDeleteProtectedNodeInBackground(client, ZKPaths.getPathAndNode(path).getPath(), protectedId).execute();
                        // assign a new id if this builder is used again later
                        protectedId = UUID.randomUUID().toString();
                    }
                }
            },
            backgrounding.getContext())
        {
            @Override
            void callPerformBackgroundOperation() throws Exception
            {
                boolean callSuper = true;
                boolean localFirstTime = firstTime.getAndSet(false) && !debugForceFindProtectedNode;
                if ( !localFirstTime && doProtected )
                {
                    debugForceFindProtectedNode = false;
                    String createdPath = null;
                    try
                    {
                        createdPath = findProtectedNodeInForeground(path);
                    }
                    catch ( KeeperException.ConnectionLossException e )
                    {
                        sendBackgroundResponse(KeeperException.Code.CONNECTIONLOSS.intValue(), path, backgrounding.getContext(), null, null, this);
                        callSuper = false;
                    }
                    if ( createdPath != null )
                    {
                        try
                        {
                            sendBackgroundResponse(KeeperException.Code.OK.intValue(), createdPath, backgrounding.getContext(), createdPath, null, this);
                        }
                        catch ( Exception e )
                        {
                            ThreadUtils.checkInterrupted(e);
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
        TimeTrace trace = client.getZookeeperClient().startTracer("CreateBuilderImpl-Foreground");

        final AtomicBoolean firstTime = new AtomicBoolean(true);
        String returnPath = RetryLoop.callWithRetry
            (
                client.getZookeeperClient(),
                new Callable<String>()
                {
                    @Override
                    public String call() throws Exception
                    {
                        boolean localFirstTime = firstTime.getAndSet(false) && !debugForceFindProtectedNode;

                        String createdPath = null;
                        if ( !localFirstTime && doProtected )
                        {
                            debugForceFindProtectedNode = false;
                            createdPath = findProtectedNodeInForeground(path);
                        }

                        if ( createdPath == null )
                        {
                            try
                            {
                                createdPath = client.getZooKeeper().create(path, data, acling.getAclList(path), createMode, storingStat);
                            }
                            catch ( KeeperException.NoNodeException e )
                            {
                                if ( createParentsIfNeeded )
                                {
                                    ZKPaths.mkdirs(client.getZooKeeper(), path, false, client.getAclProvider(), createParentsAsContainers);
                                    createdPath = client.getZooKeeper().create(path, data, acling.getAclList(path), createMode, storingStat);
                                }
                                else
                                {
                                    throw e;
                                }
                            }
                            catch ( KeeperException.NodeExistsException e )
                            {
                                if ( setDataIfExists )
                                {
                                    client.getZooKeeper().setData(path, data, -1);
                                    createdPath = path;
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

    private String findProtectedNodeInForeground(final String path) throws Exception
    {
        TimeTrace trace = client.getZookeeperClient().startTracer("CreateBuilderImpl-findProtectedNodeInForeground");

        String returnPath = RetryLoop.callWithRetry
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
                            final ZKPaths.PathAndNode pathAndNode = ZKPaths.getPathAndNode(path);
                            List<String> children = client.getZooKeeper().getChildren(pathAndNode.getPath(), false);

                            foundNode = findNode(children, pathAndNode.getPath(), protectedId);
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

    @VisibleForTesting
    String adjustPath(String path) throws Exception
    {
        if ( doProtected )
        {
            ZKPaths.PathAndNode pathAndNode = ZKPaths.getPathAndNode(path);
            String name = getProtectedPrefix(protectedId) + pathAndNode.getNode();
            path = ZKPaths.makePath(pathAndNode.getPath(), name);
        }
        return path;
    }

    /**
     * Attempt to find the znode that matches the given path and protected id
     *
     * @param children    a list of candidates znodes
     * @param path        the path
     * @param protectedId the protected id
     * @return the absolute path of the znode or <code>null</code> if it is not found
     */
    static String findNode(final List<String> children, final String path, final String protectedId)
    {
        final String protectedPrefix = getProtectedPrefix(protectedId);
        String foundNode = Iterables.find
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
            foundNode = ZKPaths.makePath(path, foundNode);
        }
        return foundNode;
    }
}
