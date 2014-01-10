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

import com.google.common.collect.Maps;
import org.apache.curator.x.websockets.ClientCreator;
import org.apache.curator.x.websockets.api.CommandManager;
import javax.websocket.Session;
import java.util.Map;

public class SessionManager
{
    private final Map<String, CuratorWebsocketsSession> sessions = Maps.newConcurrentMap();
    private final ClientCreator clientCreator;
    private final CommandManager commandManager;

    public SessionManager(ClientCreator clientCreator, CommandManager commandManager)
    {
        this.clientCreator = clientCreator;
        this.commandManager = commandManager;
    }

    public void put(Session session, CuratorWebsocketsSession curatorWebsocketsSession)
    {
        sessions.put(session.getId(), curatorWebsocketsSession);
    }

    public CuratorWebsocketsSession get(Session session)
    {
        return sessions.get(session.getId());
    }

    public CuratorWebsocketsSession remove(Session session)
    {
        return sessions.remove(session.getId());
    }

    public ClientCreator getClientCreator()
    {
        return clientCreator;
    }

    public CommandManager getCommandManager()
    {
        return commandManager;
    }
}
