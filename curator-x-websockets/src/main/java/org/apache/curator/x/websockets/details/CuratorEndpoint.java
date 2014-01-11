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

package org.apache.curator.x.websockets.details;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.x.websockets.api.ApiCommand;
import org.apache.curator.x.websockets.api.JsonUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.node.ObjectNode;
import javax.websocket.CloseReason;
import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.Session;
import java.io.IOException;

public class CuratorEndpoint extends Endpoint
{
    private final SessionManager sessionManager;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ObjectReader reader = mapper.reader();
    private final ObjectWriter writer = mapper.writer();

    public CuratorEndpoint(SessionManager sessionManager)
    {
        this.sessionManager = sessionManager;
    }

    @Override
    public void onOpen(final Session session, EndpointConfig config)
    {
        try
        {
            CuratorFramework client = sessionManager.getClientCreator().newClient();
            sessionManager.put(session, new CuratorWebsocketsSession(client, session));

            ConnectionStateListener listener = new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    try
                    {
                        ObjectNode node = mapper.createObjectNode();
                        node.put("newState", newState.name());
                        String message = JsonUtils.newMessage(mapper, writer, JsonUtils.SYSTEM_TYPE_CONNECTION_STATE_CHANGE, node);
                        session.getAsyncRemote().sendText(message);
                    }
                    catch ( Exception e )
                    {
                        // TODO
                    }
                }
            };
            client.getConnectionStateListenable().addListener(listener);

            client.start();
        }
        catch ( Exception e )
        {
            // TODO
        }

        MessageHandler handler = new MessageHandler.Whole<String>()
        {
            @Override
            public void onMessage(String message)
            {
                processMessage(session, message);
            }
        };
        session.addMessageHandler(handler);
    }

    @Override
    public void onClose(Session session, CloseReason closeReason)
    {
        CuratorWebsocketsSession curatorWebsocketsSession = sessionManager.remove(session);
        if ( curatorWebsocketsSession != null )
        {
            curatorWebsocketsSession.close();
        }
    }

    private void processMessage(Session session, String message)
    {
        try
        {
            CuratorWebsocketsSession curatorWebsocketsSession = sessionManager.get(session);
            if ( curatorWebsocketsSession == null )
            {
                throw new Exception("No session found for sessionId: " + session.getId());
            }

            JsonNode jsonNode = reader.readTree(message);
            String command = JsonUtils.getRequiredString(jsonNode, JsonUtils.FIELD_TYPE);
            String id = JsonUtils.getRequiredString(jsonNode, JsonUtils.FIELD_ID);
            JsonNode value = jsonNode.get(JsonUtils.FIELD_VALUE);

            ApiCommand apiCommand = sessionManager.getCommandManager().newCommand(command);
            apiCommand.process(id, value, curatorWebsocketsSession, reader, writer);
        }
        catch ( Exception e )
        {
            // TODO
        }
    }
}
