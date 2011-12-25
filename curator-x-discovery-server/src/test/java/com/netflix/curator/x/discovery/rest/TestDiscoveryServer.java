package com.netflix.curator.x.discovery.rest;/*
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

import com.google.common.collect.Lists;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.ServiceType;
import com.netflix.curator.x.discovery.entities.JsonServiceNamesMarshaller;
import com.netflix.curator.x.discovery.entities.ServiceInstances;
import com.netflix.curator.x.discovery.entities.ServiceNames;
import com.netflix.curator.x.discovery.payloads.string.StringDiscoveryResource;
import com.netflix.curator.x.discovery.payloads.string.StringJsonServiceInstanceMarshaller;
import com.netflix.curator.x.discovery.payloads.string.StringJsonServiceInstancesMarshaller;
import com.netflix.curator.x.discovery.rest.concretes.StringDiscoveryContextForTest;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.test.framework.AppDescriptor;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.WebAppDescriptor;
import junit.framework.Assert;
import org.junit.Test;
import javax.ws.rs.core.MediaType;

public class TestDiscoveryServer extends JerseyTest
{
    @SuppressWarnings("unchecked")
    @Override
    protected AppDescriptor configure()
    {
        String[]                        packages =
        {
            StringDiscoveryResource.class.getPackage().getName(),
            DiscoveryResource.class.getPackage().getName(),
            JsonServiceNamesMarshaller.class.getPackage().getName()
        };
        WebAppDescriptor.Builder   builder = new WebAppDescriptor.Builder(packages);

        DefaultClientConfig        config = new DefaultClientConfig();

        config.getClasses().add(StringJsonServiceInstancesMarshaller.class);
        config.getClasses().add(StringJsonServiceInstanceMarshaller.class);
        config.getClasses().add(StringDiscoveryResource.class);
        config.getClasses().add(JsonServiceNamesMarshaller.class);

        config.getSingletons().add(new StringDiscoveryContextForTest());

        builder.clientConfig(config);

        return builder.build();
    }

    @Test
    public void     testRegisterService() throws Exception
    {
        ServiceInstance<String>     service = ServiceInstance.<String>builder()
            .name("test")
            .payload("From Test")
            .serviceType(ServiceType.STATIC)
            .build();

        WebResource                 resource = resource();
        resource.path("/v1/service/test/" + service.getId()).type(MediaType.APPLICATION_JSON_TYPE).put(service);

        ServiceNames                names = resource.path("/v1/service").get(ServiceNames.class);
        Assert.assertEquals(names.getNames(), Lists.newArrayList("test"));

        GenericType<ServiceInstances<String>>   type = new GenericType<ServiceInstances<String>>(){};
        ServiceInstances<String>    instances = resource.path("/v1/service/test").get(type);
        Assert.assertEquals(instances.getServices().size(), 1);
        Assert.assertEquals(instances.getServices().get(0), service);
    }

    @Test
    public void     testEmptyServiceNames()
    {
        WebResource                 resource = resource();
        ServiceNames                names = resource.path("/v1/service").get(ServiceNames.class);
        Assert.assertEquals(names.getNames(), Lists.<String>newArrayList());
    }
}
