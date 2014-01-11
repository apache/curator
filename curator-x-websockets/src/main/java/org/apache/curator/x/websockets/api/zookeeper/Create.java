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

package org.apache.curator.x.websockets.api.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.BackgroundPathAndBytesable;
import org.apache.curator.framework.api.Compressible;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.CreateModable;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.PathAndBytesable;
import org.apache.curator.x.websockets.api.ApiCommand;
import org.apache.curator.x.websockets.api.JsonUtils;
import org.apache.curator.x.websockets.details.CuratorWebsocketsSession;
import org.apache.zookeeper.CreateMode;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.map.ObjectWriter;

public class Create implements ApiCommand
{
    @SuppressWarnings("unchecked")
    @Override
    public void process(String id, JsonNode input, CuratorWebsocketsSession session, ObjectReader reader, ObjectWriter writer) throws Exception
    {
        String path = JsonUtils.getRequiredString(input, "path");
        boolean withProtection = JsonUtils.getOptionalBoolean(input, "withProtection");
        boolean creatingParentsIfNeeded = JsonUtils.getOptionalBoolean(input, "creatingParentsIfNeeded");
        boolean compressed = JsonUtils.getOptionalBoolean(input, "compressed");
        String mode = JsonUtils.getOptionalString(input, "mode");

        JsonNode payload = input.get("payload");

        Object builder = session.getClient().create();
        Object result;
        try
        {
            if ( withProtection )
            {
                builder = ((CreateBuilder)builder).withProtection();
            }
            if ( creatingParentsIfNeeded )
            {
                builder = ((CreateBuilder)builder).creatingParentsIfNeeded();
            }
            if ( compressed )
            {
                builder = ((Compressible)builder).compressed();
            }

            if ( mode != null )
            {
                CreateMode createMode = CreateMode.valueOf(mode.toUpperCase());
                builder = ((CreateModable)builder).withMode(createMode);
            }

            BackgroundCallback callback = new BackgroundCallback()
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    System.out.println();
                }
            };
            if ( payload != null )
            {
                String payloadStr = writer.writeValueAsString(payload);
                result = ((BackgroundPathAndBytesable<String>)builder).inBackground(callback).forPath(path, payloadStr.getBytes());
            }
            else
            {
                result = ((BackgroundPathAndBytesable<String>)builder).inBackground(callback).forPath(path);
            }
        }
        catch ( ClassCastException e )
        {
            throw new Exception("Bad combination of arguments to create()");
        }

        // TODO ACL, result
    }
}
