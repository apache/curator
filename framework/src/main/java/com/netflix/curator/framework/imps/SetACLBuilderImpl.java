/*
 Copyright 2011 Netflix, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 */
package com.netflix.curator.framework.imps;

import com.netflix.curator.RetryLoop;
import com.netflix.curator.TimeTrace;
import com.netflix.curator.framework.api.*;
import com.netflix.curator.framework.api.BackgroundCallback;
import com.netflix.curator.framework.api.CuratorEventType;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.SetACLBuilder;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import java.util.List;
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
        acling = new ACLing();
        version = -1;
    }

    @Override
    public BackgroundPathable<Stat> withACL(List<ACL> aclList)
    {
        acling = new ACLing(aclList);
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
            client.processBackgroundOperation(new OperationAndData<String>(this, path, backgrounding.getCallback()), null);
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
        final TimeTrace   trace = client.getZookeeperClient().startTracer("SetACLBuilderImpl-Background");
        client.getZooKeeper().setACL
        (
            operationAndData.getData(),
            acling.getAclList(),
            version,
            new AsyncCallback.StatCallback()
            {
                @SuppressWarnings({"unchecked"})
                @Override
                public void processResult(int rc, String path, Object ctx, Stat stat)
                {
                    trace.commit();
                    CuratorEvent event = new CuratorEventImpl(CuratorEventType.SET_DATA, rc, path, null, ctx, stat, null, null, null, null);
                    client.processBackgroundOperation(operationAndData, event);
                }
            },
            backgrounding.getContext()
        );
    }

    private Stat pathInForeground(String path) throws Exception
    {
        Stat        resultStat = null;

        TimeTrace trace = client.getZookeeperClient().startTracer("SetACLBuilderImpl-Foreground");
        RetryLoop retryLoop = client.newRetryLoop();
        while ( retryLoop.shouldContinue() )
        {
            try
            {
                resultStat = client.getZooKeeper().setACL(path, acling.getAclList(), version);
                retryLoop.markComplete();
            }
            catch ( Exception e )
            {
                retryLoop.takeException(e);
            }
        }
        trace.commit();
        return resultStat;
    }
}
