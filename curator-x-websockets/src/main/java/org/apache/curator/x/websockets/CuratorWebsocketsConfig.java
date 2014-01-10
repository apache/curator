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

public class CuratorWebsocketsConfig
{
    private String bindHost = "localhost";
    private int port = 8080;
    private String rootPath = "/websockets";
    private String websocketPath = "/curator";

    public String getBindHost()
    {
        return bindHost;
    }

    public void setBindHost(String bindHost)
    {
        this.bindHost = bindHost;
    }

    public int getPort()
    {
        return port;
    }

    public void setPort(int port)
    {
        this.port = port;
    }

    public String getRootPath()
    {
        return rootPath;
    }

    public void setRootPath(String rootPath)
    {
        this.rootPath = rootPath;
    }

    public String getWebsocketPath()
    {
        return websocketPath;
    }

    public void setWebsocketPath(String websocketPath)
    {
        this.websocketPath = websocketPath;
    }
}
