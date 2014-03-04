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

package org.apache.curator.x.rest.support;

import org.apache.curator.x.rest.api.ClientResource;
import javax.ws.rs.core.UriBuilder;
import java.net.InetSocketAddress;
import java.net.URI;

public class UriMaker
{
    private final int port;

    public UriMaker(int port)
    {
        this.port = port;
    }

    public InetSocketAddress getLocalhost()
    {
        return new InetSocketAddress("localhost", port);
    }

    public int getPort()
    {
        return port;
    }

    public URI getMethodUri(Class resource, String method)
    {
        UriBuilder builder = UriBuilder.fromUri("http://localhost:" + port).path(resource);
        if ( method != null )
        {
            builder = builder.path(resource, method);
        }
        return builder.build();
    }

    public URI getMethodUri(String method)
    {
        return getMethodUri(ClientResource.class, method);
    }

    public URI getStatusUri()
    {
        return getMethodUri("getStatus");
    }
}
