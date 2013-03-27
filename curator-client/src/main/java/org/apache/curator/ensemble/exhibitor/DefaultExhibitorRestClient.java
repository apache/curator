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
package org.apache.curator.ensemble.exhibitor;

import com.google.common.io.Closeables;
import java.io.BufferedInputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;

@SuppressWarnings("UnusedDeclaration")
public class DefaultExhibitorRestClient implements ExhibitorRestClient
{
    private final boolean useSsl;

    public DefaultExhibitorRestClient()
    {
        this(false);
    }

    public DefaultExhibitorRestClient(boolean useSsl)
    {
        this.useSsl = useSsl;
    }

    @Override
    public String getRaw(String hostname, int port, String uriPath, String mimeType) throws Exception
    {
        URI                 uri = new URI(useSsl ? "https" : "http", null, hostname, port, uriPath, null, null);
        HttpURLConnection   connection = (HttpURLConnection)uri.toURL().openConnection();
        connection.addRequestProperty("Accept", mimeType);
        StringBuilder       str = new StringBuilder();
        InputStream         in = new BufferedInputStream(connection.getInputStream());
        try
        {
            for(;;)
            {
                int     b = in.read();
                if ( b < 0 )
                {
                    break;
                }
                str.append((char)(b & 0xff));
            }
        }
        finally
        {
            Closeables.closeQuietly(in);
        }
        return str.toString();
    }
}
