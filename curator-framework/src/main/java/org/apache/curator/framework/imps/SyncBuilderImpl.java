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

import java.util.concurrent.Executor;
import org.apache.curator.drivers.OperationTrace;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.ErrorListenerPathable;
import org.apache.curator.framework.api.Pathable;
import org.apache.curator.framework.api.SyncBuilder;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.zookeeper.AsyncCallback;

public class SyncBuilderImpl implements SyncBuilder, BackgroundOperation<String>, ErrorListenerPathable<Void> {
    private final CuratorFrameworkBase client;
    private Backgrounding backgrounding = new Backgrounding();

    public SyncBuilderImpl(CuratorFrameworkBase client) {
        // To change body of created methods use File | Settings | File Templates.
        this.client = client;
    }

    public SyncBuilderImpl(CuratorFrameworkBase client, Backgrounding backgrounding) {
        this.client = client;
        this.backgrounding = backgrounding;
    }

    @Override
    public ErrorListenerPathable<Void> inBackground() {
        // NOP always in background
        return this;
    }

    @Override
    public ErrorListenerPathable<Void> inBackground(Object context) {
        backgrounding = new Backgrounding(context);
        return this;
    }

    @Override
    public ErrorListenerPathable<Void> inBackground(BackgroundCallback callback) {
        backgrounding = new Backgrounding(callback);
        return this;
    }

    @Override
    public ErrorListenerPathable<Void> inBackground(BackgroundCallback callback, Object context) {
        backgrounding = new Backgrounding(callback, context);
        return this;
    }

    @Override
    public ErrorListenerPathable<Void> inBackground(BackgroundCallback callback, Executor executor) {
        backgrounding = new Backgrounding(client, callback, executor);
        return this;
    }

    @Override
    public ErrorListenerPathable<Void> inBackground(BackgroundCallback callback, Object context, Executor executor) {
        backgrounding = new Backgrounding(client, callback, context, executor);
        return this;
    }

    @Override
    public Pathable<Void> withUnhandledErrorListener(UnhandledErrorListener listener) {
        backgrounding = new Backgrounding(backgrounding, listener);
        return this;
    }

    @Override
    public CuratorEventType getBackgroundEventType() {
        return CuratorEventType.SYNC;
    }

    @Override
    public void performBackgroundOperation(final OperationAndData<String> operationAndData) throws Exception {
        try {
            final OperationTrace trace = client.getZookeeperClient().startAdvancedTracer("SyncBuilderImpl-Background");
            final String path = operationAndData.getData();
            String adjustedPath = client.fixForNamespace(path);

            AsyncCallback.VoidCallback voidCallback = new AsyncCallback.VoidCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx) {
                    trace.setReturnCode(rc).setPath(path).commit();
                    CuratorEvent event = new CuratorEventImpl(
                            client, CuratorEventType.SYNC, rc, path, path, ctx, null, null, null, null, null, null);
                    client.processBackgroundOperation(operationAndData, event);
                }
            };
            client.getZooKeeper().sync(adjustedPath, voidCallback, backgrounding.getContext());
        } catch (Throwable e) {
            backgrounding.checkError(e, null);
        }
    }

    @Override
    public Void forPath(String path) throws Exception {
        OperationAndData<String> operationAndData = new OperationAndData<String>(
                this, path, backgrounding.getCallback(), null, backgrounding.getContext(), null);
        client.processBackgroundOperation(operationAndData, null);
        return null;
    }
}
