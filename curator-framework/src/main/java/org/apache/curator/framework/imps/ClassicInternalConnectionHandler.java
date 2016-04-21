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

import org.apache.curator.framework.state.ConnectionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ClassicInternalConnectionHandler implements InternalConnectionHandler
{
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public void checkNewConnection(CuratorFrameworkImpl client)
    {
        // NOP
    }

    @Override
    public void suspendConnection(CuratorFrameworkImpl client)
    {
        if ( client.setToSuspended() )
        {
            doSyncForSuspendedConnection(client, client.getZookeeperClient().getInstanceIndex());
        }
    }

    private void doSyncForSuspendedConnection(final CuratorFrameworkImpl client, final long instanceIndex)
    {
        // we appear to have disconnected, force a new ZK event and see if we can connect to another server
        final BackgroundOperation<String> operation = new BackgroundSyncImpl(client, null);
        OperationAndData.ErrorCallback<String> errorCallback = new OperationAndData.ErrorCallback<String>()
        {
            @Override
            public void retriesExhausted(OperationAndData<String> operationAndData)
            {
                // if instanceIndex != newInstanceIndex, the ZooKeeper instance was reset/reallocated
                // so the pending background sync is no longer valid.
                // if instanceIndex is -1, this is the second try to sync - punt and mark the connection lost
                if ( (instanceIndex < 0) || (instanceIndex == client.getZookeeperClient().getInstanceIndex()) )
                {
                    client.addStateChange(ConnectionState.LOST);
                }
                else
                {
                    log.debug("suspendConnection() failure ignored as the ZooKeeper instance was reset. Retrying.");
                    // send -1 to signal that if it happens again, punt and mark the connection lost
                    doSyncForSuspendedConnection(client, -1);
                }
            }
        };
        client.performBackgroundOperation(new OperationAndData<String>(operation, "/", null, errorCallback, null, null));
    }
}
