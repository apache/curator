/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package org.apache.curator.x.discovery.server.jetty_resteasy;

import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceType;
import org.apache.curator.x.discovery.server.entity.ServiceInstances;
import org.apache.curator.x.discovery.server.entity.ServiceNames;
import junit.framework.Assert;
import org.jboss.resteasy.plugins.server.servlet.HttpServletDispatcher;
import org.jboss.resteasy.plugins.server.servlet.ResteasyBootstrap;
import org.jboss.resteasy.spi.ResteasyProviderFactory;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
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
    private Server server;

    @BeforeMethod
    public void         setup() throws Exception
    {
        RestEasyApplication.singletonsRef.set(new RestEasySingletons());

        ResteasyProviderFactory.setInstance(new ResteasyProviderFactory());

        HttpServletDispatcher   dispatcher = new HttpServletDispatcher();

        server = new Server(8080);
        Context root = new Context(server, "/", Context.SESSIONS);
        root.getInitParams().put("javax.ws.rs.Application", RestEasyApplication.class.getName());
        root.addServlet(new ServletHolder(dispatcher), "/*");
        root.addEventListener(new ResteasyBootstrap());
        server.start();
    }
    
    @AfterMethod
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

        getJson("http://localhost:8080/v1/service/test/" + service.getId(), new String(out.toByteArray()));

        String json = getJson("http://localhost:8080/v1/service", null);
        ServiceNames names = restEasySingletons.serviceNamesMarshallerSingleton.readFrom(ServiceNames.class, null, null, MediaType.APPLICATION_JSON_TYPE, null, new ByteArrayInputStream(json.getBytes()));
        Assert.assertEquals(names.getNames(), Lists.newArrayList("test"));

        json = getJson("http://localhost:8080/v1/service/test", null);
        ServiceInstances<String> instances = restEasySingletons.serviceInstancesMarshallerSingleton.readFrom(null, null, null, null, null, new ByteArrayInputStream(json.getBytes()));
        Assert.assertEquals(instances.getServices().size(), 1);
        Assert.assertEquals(instances.getServices().get(0), service);

        // Retrieve single instance
        json = getJson("http://localhost:8080/v1/service/test/" + service.getId(), null);
        ServiceInstance<String> instance = restEasySingletons.serviceInstanceMarshallerSingleton.readFrom(null, null, null, null, null, new ByteArrayInputStream(json.getBytes()));
        Assert.assertEquals(instance, service);

    }

    @Test
    public void     testEmptyServiceNames() throws Exception
    {
        String          json = getJson("http://localhost:8080/v1/service", null);
        ServiceNames    names = RestEasyApplication.singletonsRef.get().serviceNamesMarshallerSingleton.readFrom(ServiceNames.class, null, null, MediaType.APPLICATION_JSON_TYPE, null, new ByteArrayInputStream(json.getBytes()));

        Assert.assertEquals(names.getNames(), Lists.<String>newArrayList());
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
            ByteStreams.copy(ByteStreams.newInputStreamSupplier(body.getBytes()), out);
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
