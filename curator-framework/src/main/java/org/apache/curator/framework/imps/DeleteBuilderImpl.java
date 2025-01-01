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

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import org.apache.curator.RetryLoop;
import org.apache.curator.drivers.OperationTrace;
import org.apache.curator.framework.api.*;
import org.apache.curator.framework.api.transaction.OperationType;
import org.apache.curator.framework.api.transaction.TransactionDeleteBuilder;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;

public class DeleteBuilderImpl implements DeleteBuilder, BackgroundOperation<String>, ErrorListenerPathable<Void> {
    private final InternalCuratorFramework client;
    private int version;
    private Backgrounding backgrounding;
    private boolean deletingChildrenIfNeeded;
    private boolean guaranteed;
    private boolean quietly;

    @VisibleForTesting
    boolean failNextDeleteForTesting = false;

    @VisibleForTesting
    boolean failBeforeNextDeleteForTesting = false;

    DeleteBuilderImpl(InternalCuratorFramework client) {
        this.client = client;
        version = -1;
        backgrounding = new Backgrounding();
        deletingChildrenIfNeeded = false;
        guaranteed = false;
        quietly = false;
    }

    public DeleteBuilderImpl(
            InternalCuratorFramework client,
            int version,
            Backgrounding backgrounding,
            boolean deletingChildrenIfNeeded,
            boolean guaranteed,
            boolean quietly) {
        this.client = client;
        this.version = version;
        this.backgrounding = backgrounding;
        this.deletingChildrenIfNeeded = deletingChildrenIfNeeded;
        this.guaranteed = guaranteed;
        this.quietly = quietly;
    }

    <T> TransactionDeleteBuilder<T> asTransactionDeleteBuilder(
            final T context, final CuratorMultiTransactionRecord transaction) {
        return new TransactionDeleteBuilder<T>() {
            @Override
            public T forPath(String path) throws Exception {
                String fixedPath = client.fixForNamespace(path);
                transaction.add(Op.delete(fixedPath, version), OperationType.DELETE, path);
                return context;
            }

            @Override
            public Pathable<T> withVersion(int version) {
                DeleteBuilderImpl.this.withVersion(version);
                return this;
            }
        };
    }

    @Override
    public DeleteBuilderMain quietly() {
        quietly = true;
        return this;
    }

    @Override
    public DeleteBuilderMain idempotent() {
        // idempotent == quietly for deletes, but keep the interface to be consistent with Create/SetData
        return quietly();
    }

    @Override
    public ChildrenDeletable guaranteed() {
        guaranteed = true;
        return this;
    }

    @Override
    public BackgroundVersionable deletingChildrenIfNeeded() {
        deletingChildrenIfNeeded = true;
        return this;
    }

    @Override
    public BackgroundPathable<Void> withVersion(int version) {
        this.version = version;
        return this;
    }

    @Override
    public ErrorListenerPathable<Void> inBackground(BackgroundCallback callback, Object context) {
        backgrounding = new Backgrounding(callback, context);
        return this;
    }

    @Override
    public ErrorListenerPathable<Void> inBackground(BackgroundCallback callback, Object context, Executor executor) {
        backgrounding = new Backgrounding(client, callback, context, executor);
        return this;
    }

    @Override
    public ErrorListenerPathable<Void> inBackground(BackgroundCallback callback) {
        backgrounding = new Backgrounding(callback);
        return this;
    }

    @Override
    public ErrorListenerPathable<Void> inBackground(BackgroundCallback callback, Executor executor) {
        backgrounding = new Backgrounding(client, callback, executor);
        return this;
    }

    @Override
    public ErrorListenerPathable<Void> inBackground() {
        backgrounding = new Backgrounding(true);
        return this;
    }

    @Override
    public ErrorListenerPathable<Void> inBackground(Object context) {
        backgrounding = new Backgrounding(context);
        return this;
    }

    @Override
    public Pathable<Void> withUnhandledErrorListener(UnhandledErrorListener listener) {
        backgrounding = new Backgrounding(backgrounding, listener);
        return this;
    }

    @Override
    public CuratorEventType getBackgroundEventType() {
        return CuratorEventType.DELETE;
    }

    @Override
    public void performBackgroundOperation(final OperationAndData<String> operationAndData) throws Exception {
        try {
            final OperationTrace trace =
                    client.getZookeeperClient().startAdvancedTracer("DeleteBuilderImpl-Background");
            client.getZooKeeper()
                    .delete(
                            operationAndData.getData(),
                            version,
                            new AsyncCallback.VoidCallback() {
                                @Override
                                public void processResult(int rc, String path, Object ctx) {
                                    trace.setReturnCode(rc).setPath(path).commit();

                                    if ((rc == KeeperException.Code.OK.intValue()) && failNextDeleteForTesting) {
                                        failNextDeleteForTesting = false;
                                        rc = KeeperException.Code.CONNECTIONLOSS.intValue();
                                    }
                                    if ((rc == KeeperException.Code.NOTEMPTY.intValue()) && deletingChildrenIfNeeded) {
                                        backgroundDeleteChildrenThenNode(operationAndData);
                                    } else {
                                        if ((rc == KeeperException.Code.NONODE.intValue()) && quietly) {
                                            rc = KeeperException.Code.OK.intValue();
                                        }
                                        CuratorEvent event = new CuratorEventImpl(
                                                client,
                                                CuratorEventType.DELETE,
                                                rc,
                                                path,
                                                null,
                                                ctx,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null);
                                        client.processBackgroundOperation(operationAndData, event);
                                    }
                                }
                            },
                            backgrounding.getContext());
        } catch (Throwable e) {
            backgrounding.checkError(e, null);
        }
    }

    private void backgroundDeleteChildrenThenNode(final OperationAndData<String> mainOperationAndData) {
        BackgroundOperation<String> operation = new BackgroundOperation<String>() {
            @Override
            public void performBackgroundOperation(OperationAndData<String> dummy) throws Exception {
                try {
                    ZKPaths.deleteChildren(client.getZooKeeper(), mainOperationAndData.getData(), false);
                } catch (KeeperException e) {
                    // ignore
                }
                client.queueOperation(mainOperationAndData);
            }

            @Override
            public CuratorEventType getBackgroundEventType() {
                return CuratorEventType.DELETE;
            }
        };
        OperationAndData<String> parentOperation = new OperationAndData<>(operation, mainOperationAndData);
        client.queueOperation(parentOperation);
    }

    @Override
    public Void forPath(String path) throws Exception {
        client.getSchemaSet().getSchema(path).validateDelete(path);

        final String unfixedPath = path;
        path = client.fixForNamespace(path);

        if (backgrounding.inBackground()) {
            OperationAndData.ErrorCallback<String> errorCallback = null;
            if (guaranteed) {
                errorCallback = new OperationAndData.ErrorCallback<String>() {
                    @Override
                    public void retriesExhausted(OperationAndData<String> operationAndData) {
                        client.getFailedDeleteManager().addFailedOperation(unfixedPath);
                    }
                };
            }
            OperationAndData<String> operationAndData =
                    new OperationAndData<String>(
                            this, path, backgrounding.getCallback(), errorCallback, backgrounding.getContext(), null) {
                        @Override
                        void callPerformBackgroundOperation() throws Exception {
                            // inject fault before performing operation
                            if (failBeforeNextDeleteForTesting) {
                                failBeforeNextDeleteForTesting = false;
                                throw new KeeperException.ConnectionLossException();
                            }
                            super.callPerformBackgroundOperation();
                        }
                    };
            client.processBackgroundOperation(operationAndData, null);
        } else {
            pathInForeground(path, unfixedPath);
        }
        return null;
    }

    protected int getVersion() {
        return version;
    }

    private void pathInForeground(final String path, String unfixedPath) throws Exception {
        OperationTrace trace = client.getZookeeperClient().startAdvancedTracer("DeleteBuilderImpl-Foreground");
        try {
            RetryLoop.callWithRetry(client.getZookeeperClient(), new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    if (failBeforeNextDeleteForTesting) {
                        failBeforeNextDeleteForTesting = false;
                        throw new KeeperException.ConnectionLossException();
                    }

                    try {
                        client.getZooKeeper().delete(path, version);
                    } catch (KeeperException.NoNodeException e) {
                        if (!quietly) {
                            throw e;
                        }
                    } catch (KeeperException.NotEmptyException e) {
                        if (deletingChildrenIfNeeded) {
                            ZKPaths.deleteChildren(client.getZooKeeper(), path, true);
                        } else {
                            throw e;
                        }
                    }

                    if (failNextDeleteForTesting) {
                        failNextDeleteForTesting = false;
                        throw new KeeperException.ConnectionLossException();
                    }

                    return null;
                }
            });
        } catch (Exception e) {
            ThreadUtils.checkInterrupted(e);
            // Only retry a guaranteed delete if it's a retryable error
            if ((client.getZookeeperClient().getRetryPolicy().allowRetry(e) || (e instanceof InterruptedException))
                    && guaranteed) {
                client.getFailedDeleteManager().addFailedOperation(unfixedPath);
            }
            throw e;
        }
        trace.setPath(path).commit();
    }
}
