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
package org.apache.curator.x.discovery.server.jetty_resteasy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import com.google.common.collect.Lists;
import com.google.common.io.ByteSource;
import com.google.common.io.CharStreams;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceType;
import org.apache.curator.x.discovery.server.entity.ServiceInstances;
import org.apache.curator.x.discovery.server.entity.ServiceNames;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.jboss.resteasy.plugins.server.servlet.HttpServletDispatcher;
import org.jboss.resteasy.plugins.server.servlet.ResteasyBootstrap;
import org.jboss.resteasy.spi.ResteasyProviderFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import javax.ws.rs.core.MediaType;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

@SuppressWarnings("unchecked")
public class TestStringsWithRestEasy
{
    private static final String HOST = "127.0.0.1";
    private Server server;
    private int port;

    @BeforeEach
    public void         setup() throws Exception
    {
        RestEasyApplication.singletonsRef.set(new RestEasySingletons());

        ResteasyProviderFactory.setInstance(new ResteasyProviderFactory());

        HttpServletDispatcher   dispatcher = new HttpServletDispatcher();

        port = InstanceSpec.getRandomPort();
        server = new Server(port);
        ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
        root.setContextPath("/");
        server.setHandler(root);
        root.setServer(server);
        root.setContextPath("/");
        root.getInitParams().put("javax.ws.rs.Application", RestEasyApplication.class.getName());
        root.addServlet(new ServletHolder(dispatcher), "/*");
        root.addEventListener(new ResteasyBootstrap());
        server.start();
    }

    @AfterEach
    public void         teardown() throws Exception
    {
        server.stop();
        server.join();
    }

    @Test
    public void     testRegisterService() throws Exception
    {
        RestEasySingletons  restEasySingletons = RestEasyApplication.singletonsRef.get();

        ServiceInstance<String> service = ServiceInstance.<String>builder()
            .name("test")
            .payload("From Test")
            .serviceType(ServiceType.STATIC)
            .build();

        ByteArrayOutputStream           out = new ByteArrayOutputStream();
        restEasySingletons.serviceInstanceMarshallerSingleton.writeTo(service, null, null, null, null, null, out);

        getJson("http://" + HOST + ":" + port + "/v1/service/test/" + service.getId(), new String(out.toByteArray()));

        String json = getJson("http://" + HOST + ":" + port + "/v1/service", null);
        ServiceNames names = restEasySingletons.serviceNamesMarshallerSingleton.readFrom(ServiceNames.class, null, null, MediaType.APPLICATION_JSON_TYPE, null, new ByteArrayInputStream(json.getBytes()));
        assertEquals(names.getNames(), Lists.newArrayList("test"));

        json = getJson("http://" + HOST + ":" + port + "/v1/service/test", null);
        ServiceInstances<String> instances = restEasySingletons.serviceInstancesMarshallerSingleton.readFrom(null, null, null, null, null, new ByteArrayInputStream(json.getBytes()));
        assertEquals(instances.getServices().size(), 1);
        assertEquals(instances.getServices().get(0), service);

        // Retrieve single instance
        json = getJson("http://" + HOST + ":" + port + "/v1/service/test/" + service.getId(), null);
        ServiceInstance<String> instance = restEasySingletons.serviceInstanceMarshallerSingleton.readFrom(null, null, null, null, null, new ByteArrayInputStream(json.getBytes()));
        assertEquals(instance, service);

    }

    @Test
    public void     testEmptyServiceNames() throws Exception
    {
        String          json = getJson("http://" + HOST + ":" + port + "/v1/service", null);
        ServiceNames    names = RestEasyApplication.singletonsRef.get().serviceNamesMarshallerSingleton.readFrom(ServiceNames.class, null, null, MediaType.APPLICATION_JSON_TYPE, null, new ByteArrayInputStream(json.getBytes()));

        assertEquals(names.getNames(), Lists.<String>newArrayList());
    }

    private String getJson(String urlStr, String body) throws IOException
    {
        URL                 url = new URL(urlStr);
        URLConnection       urlConnection = url.openConnection();
        urlConnection.addRequestProperty("Accept", "application/json");
        if ( body != null )
        {
            ((HttpURLConnection)urlConnection).setRequestMethod("PUT");

            urlConnection.addRequestProperty("Content-Type", "application/json");
            urlConnection.addRequestProperty("Content-Length", Integer.toString(body.length()));
            urlConnection.setDoOutput(true);

            OutputStream        out = urlConnection.getOutputStream();
            ByteSource.wrap(body.getBytes()).copyTo(out);
        }
        BufferedReader in = new BufferedReader(new InputStreamReader(urlConnection.getInputStream()));
        try
        {
            return CharStreams.toString(in);
        }
        finally
        {
            in.close();
        }
    }

}
