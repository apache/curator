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
import org.apache.curator.x.websockets.api.ApiCommand;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.map.ObjectWriter;
import javax.websocket.CloseReason;
import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.Session;
import java.io.IOException;

public class CuratorEndpoint extends Endpoint
{
    private final SessionManager sessionManager;
    private final ObjectReader reader = new ObjectMapper().reader();
    private final ObjectWriter writer = new ObjectMapper().writer();

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
            JsonNode command = jsonNode.get("command");
            if ( command == null )
            {
                throw new Exception("Missing field: \"command\"");
            }
            String commandName = command.asText();
            ApiCommand apiCommand = sessionManager.getCommandManager().newCommand(commandName);
            apiCommand.process(jsonNode, curatorWebsocketsSession, reader, writer);
        }
        catch ( Exception e )
        {
            // TODO
        }
    }
}
