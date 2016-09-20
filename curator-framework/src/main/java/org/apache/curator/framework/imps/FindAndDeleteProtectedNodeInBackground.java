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
import org.apache.curator.drivers.OperationTrace;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

class FindAndDeleteProtectedNodeInBackground implements BackgroundOperation<Void>
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorFrameworkImpl client;
    private final String namespaceAdjustedParentPath;
    private final String protectedId;

    FindAndDeleteProtectedNodeInBackground(CuratorFrameworkImpl client, String namespaceAdjustedParentPath, String protectedId)
    {
        this.client = client;
        this.namespaceAdjustedParentPath = namespaceAdjustedParentPath;
        this.protectedId = protectedId;
    }

    void execute()
    {
        OperationAndData.ErrorCallback<Void> errorCallback = new OperationAndData.ErrorCallback<Void>()
        {
            @Override
            public void retriesExhausted(OperationAndData<Void> operationAndData)
            {
                operationAndData.reset();
                client.processBackgroundOperation(operationAndData, null);
            }
        };
        OperationAndData<Void> operationAndData = new OperationAndData<Void>(this, null, null, errorCallback, null);
        client.processBackgroundOperation(operationAndData, null);
    }

    @VisibleForTesting
    static final AtomicBoolean debugInsertError = new AtomicBoolean(false);

    @Override
    public void performBackgroundOperation(final OperationAndData<Void> operationAndData) throws Exception
    {
        final OperationTrace trace = client.getZookeeperClient().startTracer("FindAndDeleteProtectedNodeInBackground");
        AsyncCallback.Children2Callback callback = new AsyncCallback.Children2Callback()
        {
            @Override
            public void processResult(int rc, String path, Object o, List<String> strings, Stat stat)
            {
                trace.commit();

                if ( debugInsertError.compareAndSet(true, false) )
                {
                    rc = KeeperException.Code.CONNECTIONLOSS.intValue();
                }

                if ( rc == KeeperException.Code.OK.intValue() )
                {
                    final String node = CreateBuilderImpl.findNode(strings, "/", protectedId);  // due to namespacing, don't let CreateBuilderImpl.findNode adjust the path
                    if ( node != null )
                    {
                        try
                        {
                            String deletePath = client.unfixForNamespace(ZKPaths.makePath(namespaceAdjustedParentPath, node));
                            client.delete().guaranteed().inBackground().forPath(deletePath);
                        }
                        catch ( Exception e )
                        {
                            ThreadUtils.checkInterrupted(e);
                            log.error("Could not start guaranteed delete for node: " + node);
                            rc = KeeperException.Code.CONNECTIONLOSS.intValue();
                        }
                    }
                }

                if ( rc != KeeperException.Code.OK.intValue() )
                {
                    CuratorEventImpl event = new CuratorEventImpl(client, CuratorEventType.CHILDREN, rc, path, null, o, stat, null, strings, null, null);
                    client.processBackgroundOperation(operationAndData, event);
                }
            }
        };
        client.getZooKeeper().getChildren(namespaceAdjustedParentPath, false, callback, null);
    }
}
