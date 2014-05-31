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
package org.apache.curator.x.rpc.details;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.x.rpc.idl.structs.RpcCuratorEvent;
import org.apache.curator.x.rpc.idl.structs.CuratorProjection;
import org.apache.curator.x.rpc.idl.services.CuratorProjectionService;

public class RpcBackgroundCallback implements BackgroundCallback
{
    private final CuratorProjection projection;
    private final CuratorProjectionService projectionService;

    public RpcBackgroundCallback(CuratorProjectionService projectionService, CuratorProjection projection)
    {
        this.projection = projection;
        this.projectionService = projectionService;
    }

    @Override
    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
    {
        projectionService.addEvent(projection, new RpcCuratorEvent(event));
    }
}
