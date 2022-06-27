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

import static org.junit.jupiter.api.Assertions.assertEquals;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import java.util.Map;
import java.util.Set;

public class TestMapsWithJersey
{
    private static final String HOST = "127.0.0.1";
    private Server server;
    private JsonServiceNamesMarshaller serviceNamesMarshaller;
    private JsonServiceInstanceMarshaller<Map<String, String>> serviceInstanceMarshaller;
    private JsonServiceInstancesMarshaller<Map<String, String>> serviceInstancesMarshaller;
    private MapDiscoveryContext context;
    private int port;

    @BeforeEach
    public void         setup() throws Exception
    {
        context = new MapDiscoveryContext(new MockServiceDiscovery<Map<String, String>>(), new RandomStrategy<Map<String, String>>(), 1000);
        serviceNamesMarshaller = new JsonServiceNamesMarshaller();
        serviceInstanceMarshaller = new JsonServiceInstanceMarshaller<Map<String, String>>(context);
        serviceInstancesMarshaller = new JsonServiceInstancesMarshaller<Map<String, String>>(context);

        Application                                     application = new Application()
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
        ServletContainer container = new ServletContainer(ResourceConfig.forApplication(application));

        port = InstanceSpec.getRandomPort();
        server = new Server(port);
        ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
        root.setContextPath("/");
        final ServletHolder servletHolder = new ServletHolder(container);
        root.addServlet(servletHolder, "/*");
        servletHolder.setInitOrder(1);
        server.setHandler(root);
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
        Map<String, String>         payload = Maps.newHashMap();
        payload.put("one", "1");
        payload.put("two", "2");
        payload.put("three", "3");
        ServiceInstance<Map<String, String>> service = ServiceInstance.<Map<String, String>>builder()
            .name("test")
            .payload(payload)
            .serviceType(ServiceType.STATIC)
            .build();

        Set<Object> singletons = Sets.newHashSet();
        singletons.add(context);
        singletons.add(serviceNamesMarshaller);
        singletons.add(serviceInstanceMarshaller);
        singletons.add(serviceInstancesMarshaller);

        ClientConfig config = new ClientConfig(singletons);
        Client client = ClientBuilder.newClient(config);
        WebTarget resource = client.target("http://" + HOST + ":" + port);
        resource.path("/v1/service/test/" + service.getId())
          .request(MediaType.APPLICATION_JSON_TYPE)
          .put(Entity.json(service));

        ServiceNames names = resource.path("/v1/service").request().get(ServiceNames.class);
        System.out.println(names);
        assertEquals(names.getNames(), Lists.newArrayList("test"));

        GenericType<ServiceInstances<Map<String, String>>> type = new GenericType<ServiceInstances<Map<String, String>>>(){};
        ServiceInstances<Map<String, String>>    instances = resource.path("/v1/service/test").request(MediaType.APPLICATION_JSON_TYPE).get(type);
        assertEquals(instances.getServices().size(), 1);
        assertEquals(instances.getServices().get(0), service);
        assertEquals(instances.getServices().get(0).getPayload(), payload);

        // Retrieve a single instance
        GenericType<ServiceInstance<Map<String, String>>> singleInstanceType = new GenericType<ServiceInstance<Map<String, String>>>(){};
        ServiceInstance<Map<String, String>>    instance = resource.path("/v1/service/test/" + service.getId()).request(MediaType.APPLICATION_JSON_TYPE).get(singleInstanceType);
        assertEquals(instance, service);

    }
}
