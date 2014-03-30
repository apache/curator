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
package org.apache.curator.x.discovery.server.jetty_jersey;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.core.DefaultResourceConfig;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceType;
import org.apache.curator.x.discovery.server.contexts.MapDiscoveryContext;
import org.apache.curator.x.discovery.server.entity.JsonServiceInstanceMarshaller;
import org.apache.curator.x.discovery.server.entity.JsonServiceInstancesMarshaller;
import org.apache.curator.x.discovery.server.entity.JsonServiceNamesMarshaller;
import org.apache.curator.x.discovery.server.entity.ServiceInstances;
import org.apache.curator.x.discovery.server.entity.ServiceNames;
import org.apache.curator.x.discovery.server.mocks.MockServiceDiscovery;
import org.apache.curator.x.discovery.strategies.RandomStrategy;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import java.util.Map;
import java.util.Set;

public class TestMapsWithJersey
{
    private Server server;
    private JsonServiceNamesMarshaller serviceNamesMarshaller;
    private JsonServiceInstanceMarshaller<Map<String, String>> serviceInstanceMarshaller;
    private JsonServiceInstancesMarshaller<Map<String, String>> serviceInstancesMarshaller;
    private MapDiscoveryContext context;
    private int port;

    @BeforeMethod
    public void         setup() throws Exception
    {
        context = new MapDiscoveryContext(new MockServiceDiscovery<Map<String, String>>(), new RandomStrategy<Map<String, String>>(), 1000);
        serviceNamesMarshaller = new JsonServiceNamesMarshaller();
        serviceInstanceMarshaller = new JsonServiceInstanceMarshaller<Map<String, String>>(context);
        serviceInstancesMarshaller = new JsonServiceInstancesMarshaller<Map<String, String>>(context);

        Application                                     application = new DefaultResourceConfig()
        {
            @Override
            public Set<Class<?>> getClasses()
            {
                Set<Class<?>>       classes = Sets.newHashSet();
                classes.add(MapDiscoveryResource.class);
                return classes;
            }

            @Override
            public Set<Object> getSingletons()
            {
                Set<Object>     singletons = Sets.newHashSet();
                singletons.add(context);
                singletons.add(serviceNamesMarshaller);
                singletons.add(serviceInstanceMarshaller);
                singletons.add(serviceInstancesMarshaller);
                return singletons;
            }
        };
        ServletContainer        container = new ServletContainer(application);

        port = InstanceSpec.getRandomPort();
        server = new Server(port);
        Context root = new Context(server, "/", Context.SESSIONS);
        root.addServlet(new ServletHolder(container), "/*");
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
        Map<String, String>         payload = Maps.newHashMap();
        payload.put("one", "1");
        payload.put("two", "2");
        payload.put("three", "3");
        ServiceInstance<Map<String, String>> service = ServiceInstance.<Map<String, String>>builder()
            .name("test")
            .payload(payload)
            .serviceType(ServiceType.STATIC)
            .build();

        ClientConfig    config = new DefaultClientConfig()
        {
            @Override
            public Set<Object> getSingletons()
            {
                Set<Object>     singletons = Sets.newHashSet();
                singletons.add(context);
                singletons.add(serviceNamesMarshaller);
                singletons.add(serviceInstanceMarshaller);
                singletons.add(serviceInstancesMarshaller);
                return singletons;
            }
        };
        Client          client = Client.create(config);
        WebResource     resource = client.resource("http://localhost:" + port);
        resource.path("/v1/service/test/" + service.getId()).type(MediaType.APPLICATION_JSON_TYPE).put(service);

        ServiceNames names = resource.path("/v1/service").get(ServiceNames.class);
        Assert.assertEquals(names.getNames(), Lists.newArrayList("test"));

        GenericType<ServiceInstances<Map<String, String>>> type = new GenericType<ServiceInstances<Map<String, String>>>(){};
        ServiceInstances<Map<String, String>>    instances = resource.path("/v1/service/test").get(type);
        Assert.assertEquals(instances.getServices().size(), 1);
        Assert.assertEquals(instances.getServices().get(0), service);
        Assert.assertEquals(instances.getServices().get(0).getPayload(), payload);

        // Retrieve a single instance
        GenericType<ServiceInstance<Map<String, String>>> singleInstanceType = new GenericType<ServiceInstance<Map<String, String>>>(){};
        ServiceInstance<Map<String, String>>    instance = resource.path("/v1/service/test/" + service.getId()).get(singleInstanceType);
        Assert.assertEquals(instance, service);

    }
}
