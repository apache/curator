/*
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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import org.apache.curator.RetryLoop;
import org.apache.curator.drivers.OperationTrace;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.ErrorListenerPathable;
import org.apache.curator.framework.api.GetACLBuilder;
import org.apache.curator.framework.api.Pathable;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public class GetACLBuilderImpl implements GetACLBuilder, BackgroundOperation<String>, ErrorListenerPathable<List<ACL>> {
    private final CuratorFrameworkBase client;

    private Backgrounding backgrounding;
    private Stat responseStat;

    GetACLBuilderImpl(CuratorFrameworkBase client) {
        this.client = client;
        backgrounding = new Backgrounding();
        responseStat = new Stat();
    }

    public GetACLBuilderImpl(CuratorFrameworkBase client, Backgrounding backgrounding, Stat responseStat) {
        this.client = client;
        this.backgrounding = backgrounding;
        this.responseStat = responseStat;
    }

    @Override
    public ErrorListenerPathable<List<ACL>> inBackground(BackgroundCallback callback, Object context) {
        backgrounding = new Backgrounding(callback, context);
        return this;
    }

    @Override
    public ErrorListenerPathable<List<ACL>> inBackground(
            BackgroundCallback callback, Object context, Executor executor) {
        backgrounding = new Backgrounding(client, callback, context, executor);
        return this;
    }

    @Override
    public ErrorListenerPathable<List<ACL>> inBackground() {
        backgrounding = new Backgrounding(true);
        return this;
    }

    @Override
    public ErrorListenerPathable<List<ACL>> inBackground(Object context) {
        backgrounding = new Backgrounding(context);
        return this;
    }

    @Override
    public ErrorListenerPathable<List<ACL>> inBackground(BackgroundCallback callback) {
        backgrounding = new Backgrounding(callback);
        return this;
    }

    @Override
    public ErrorListenerPathable<List<ACL>> inBackground(BackgroundCallback callback, Executor executor) {
        backgrounding = new Backgrounding(client, callback, executor);
        return this;
    }

    @Override
    public Pathable<List<ACL>> withUnhandledErrorListener(UnhandledErrorListener listener) {
        backgrounding = new Backgrounding(backgrounding, listener);
        return this;
    }

    @Override
    public Pathable<List<ACL>> storingStatIn(Stat stat) {
        responseStat = stat;
        return this;
    }

    @Override
    public CuratorEventType getBackgroundEventType() {
        return CuratorEventType.GET_ACL;
    }

    @Override
    public void performBackgroundOperation(final OperationAndData<String> operationAndData) throws Exception {
        try {
            final OperationTrace trace =
                    client.getZookeeperClient().startAdvancedTracer("GetACLBuilderImpl-Background");
            AsyncCallback.ACLCallback callback = new AsyncCallback.ACLCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, List<ACL> acl, Stat stat) {
                    trace.setReturnCode(rc).setPath(path).setStat(stat).commit();
                    CuratorEventImpl event = new CuratorEventImpl(
                            client, CuratorEventType.GET_ACL, rc, path, null, ctx, stat, null, null, null, acl, null);
                    client.processBackgroundOperation(operationAndData, event);
                }
            };
            client.getZooKeeper()
                    .getACL(operationAndData.getData(), responseStat, callback, backgrounding.getContext());
        } catch (Throwable e) {
            backgrounding.checkError(e, null);
        }
    }

    @Override
    public List<ACL> forPath(String path) throws Exception {
        path = client.fixForNamespace(path);

        List<ACL> result = null;
        if (backgrounding.inBackground()) {
            client.processBackgroundOperation(
                    new OperationAndData<String>(
                            this, path, backgrounding.getCallback(), null, backgrounding.getContext(), null),
                    null);
        } else {
            result = pathInForeground(path);
        }
        return result;
    }

    private List<ACL> pathInForeground(final String path) throws Exception {
        OperationTrace trace = client.getZookeeperClient().startAdvancedTracer("GetACLBuilderImpl-Foreground");
        List<ACL> result = RetryLoop.callWithRetry(client.getZookeeperClient(), new Callable<List<ACL>>() {
            @Override
            public List<ACL> call() throws Exception {
                return client.getZooKeeper().getACL(path, responseStat);
            }
        });
        trace.setPath(path).setStat(responseStat).commit();
        return result;
    }
}
