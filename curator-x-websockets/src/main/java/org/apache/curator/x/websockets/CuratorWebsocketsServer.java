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

package org.apache.curator.x.websockets;

import com.google.common.base.Preconditions;
import org.apache.curator.x.websockets.api.CommandManager;
import org.apache.curator.x.websockets.details.CuratorEndpoint;
import org.apache.curator.x.websockets.details.SessionManager;
import org.glassfish.tyrus.spi.ServerContainer;
import org.glassfish.tyrus.spi.ServerContainerFactory;
import javax.websocket.server.ServerEndpointConfig;
import java.io.Closeable;
import java.util.List;

public class CuratorWebsocketsServer implements Closeable
{
    private final ServerContainer serverContainer;
    private final String rootPath;
    private final int port;
    private final CommandManager commandManager = new CommandManager();

    public CuratorWebsocketsServer(CuratorWebsocketsConfig config, ClientCreator clientCreator) throws Exception
    {
        rootPath = config.getRootPath();
        port = config.getPort();

        serverContainer = ServerContainerFactory.createServerContainer(null);

        final SessionManager sessionManager = new SessionManager(clientCreator, commandManager);
        ServerEndpointConfig.Configurator configurator = new ServerEndpointConfig.Configurator()
        {
            @Override
            public String getNegotiatedSubprotocol(List<String> supported, List<String> requested)
            {
                return super.getNegotiatedSubprotocol(supported, requested);
            }

            @Override
            public <T> T getEndpointInstance(Class<T> endpointClass) throws InstantiationException
            {
                Preconditions.checkArgument(endpointClass.equals(CuratorEndpoint.class), "Expected CuratorEndpoint: " + endpointClass.getName());
                //noinspection unchecked
                return (T)new CuratorEndpoint(sessionManager);
            }
        };
        ServerEndpointConfig serverEndpointConfig = ServerEndpointConfig.Builder.create(CuratorEndpoint.class, config.getWebsocketPath()).configurator(configurator).build();
        serverContainer.addEndpoint(serverEndpointConfig);
    }

    public void start() throws Exception
    {
        serverContainer.start(rootPath, port);
    }

    @Override
    public void close()
    {
        serverContainer.stop();
    }
}
