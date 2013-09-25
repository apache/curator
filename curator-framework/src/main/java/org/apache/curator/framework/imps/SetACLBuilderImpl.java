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
import org.apache.curator.TimeTrace;
import org.apache.curator.framework.api.*;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.SetACLBuilder;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

class SetACLBuilderImpl implements SetACLBuilder, BackgroundPathable<Stat>, BackgroundOperation<String>
{
    private final CuratorFrameworkImpl client;

    private ACLing              acling;
    private Backgrounding       backgrounding;
    private int                 version;

    SetACLBuilderImpl(CuratorFrameworkImpl client)
    {
        this.client = client;
        backgrounding = new Backgrounding();
        acling = new ACLing(client.getAclProvider());
        version = -1;
    }

    @Override
    public BackgroundPathable<Stat> withACL(List<ACL> aclList)
    {
        acling = new ACLing(client.getAclProvider(), aclList);
        return this;
    }

    @Override
    public ACLable<BackgroundPathable<Stat>> withVersion(int version)
    {
        this.version = version;
        return this;
    }

    @Override
    public Pathable<Stat> inBackground()
    {
        backgrounding = new Backgrounding(true);
        return this;
    }

    @Override
    public Pathable<Stat> inBackground(Object context)
    {
        backgrounding = new Backgrounding(context);
        return this;
    }

    @Override
    public Pathable<Stat> inBackground(BackgroundCallback callback)
    {
        backgrounding = new Backgrounding(callback);
        return this;
    }

    @Override
    public Pathable<Stat> inBackground(BackgroundCallback callback, Object context)
    {
        backgrounding = new Backgrounding(callback, context);
        return this;
    }

    @Override
    public Pathable<Stat> inBackground(BackgroundCallback callback, Object context, Executor executor)
    {
        backgrounding = new Backgrounding(client, callback, context, executor);
        return this;
    }

    @Override
    public Pathable<Stat> inBackground(BackgroundCallback callback, Executor executor)
    {
        backgrounding = new Backgrounding(client, callback, executor);
        return this;
    }

    @Override
    public Stat forPath(String path) throws Exception
    {
        path = client.fixForNamespace(path);

        Stat        resultStat = null;
        if ( backgrounding.inBackground()  )
        {
            client.processBackgroundOperation(new OperationAndData<String>(this, path, backgrounding.getCallback(), null, backgrounding.getContext()), null);
        }
        else
        {
            resultStat = pathInForeground(path);
        }
        return resultStat;
    }

    @Override
    public void performBackgroundOperation(final OperationAndData<String> operationAndData) throws Exception
    {
        final TimeTrace     trace = client.getZookeeperClient().startTracer("SetACLBuilderImpl-Background");
        String              path = operationAndData.getData();
        client.getZooKeeper().setACL
        (
            path,
            acling.getAclList(path),
            version,
            new AsyncCallback.StatCallback()
            {
                @SuppressWarnings({"unchecked"})
                @Override
                public void processResult(int rc, String path, Object ctx, Stat stat)
                {
                    trace.commit();
                    CuratorEvent event = new CuratorEventImpl(client, CuratorEventType.SET_ACL, rc, path, null, ctx, stat, null, null, null, null);
                    client.processBackgroundOperation(operationAndData, event);
                }
            },
            backgrounding.getContext()
        );
    }

    private Stat pathInForeground(final String path) throws Exception
    {
        TimeTrace   trace = client.getZookeeperClient().startTracer("SetACLBuilderImpl-Foreground");
        Stat        resultStat = RetryLoop.callWithRetry
        (
            client.getZookeeperClient(),
            new Callable<Stat>()
            {
                @Override
                public Stat call() throws Exception
                {
                    return client.getZooKeeper().setACL(path, acling.getAclList(path), version);
                }
            }
        );
        trace.commit();
        return resultStat;
    }
}
