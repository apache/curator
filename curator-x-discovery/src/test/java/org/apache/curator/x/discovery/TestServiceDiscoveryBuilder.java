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
package org.apache.curator.x.discovery;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.InstanceSerializer;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.curator.x.discovery.details.ServiceDiscoveryImpl;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestServiceDiscoveryBuilder extends BaseClassForTests
{
    @Test
    public void testDefaultSerializer() throws Exception
    {        
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        ServiceDiscoveryBuilder<Object> builder = ServiceDiscoveryBuilder.builder(Object.class).client(client);
        ServiceDiscoveryImpl<?> discovery = (ServiceDiscoveryImpl<?>) builder.basePath("/path").build();

        Assert.assertNotNull(discovery.getSerializer(), "default serializer not set");
        Assert.assertTrue(discovery.getSerializer() instanceof JsonInstanceSerializer, "default serializer not JSON");
    }

    @Test
    public void testSetSerializer() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        ServiceDiscoveryBuilder<Object> builder = ServiceDiscoveryBuilder.builder(Object.class).client(client);
        builder.serializer(new InstanceSerializer<Object>()
        {
            @Override
            public byte[] serialize(ServiceInstance<Object> instance)
            {
                return null;
            }

            @Override
            public ServiceInstance<Object> deserialize(byte[] bytes)
            {
                return null;
            }
        });

        ServiceDiscoveryImpl<?> discovery = (ServiceDiscoveryImpl<?>) builder.basePath("/path").build();
        Assert.assertNotNull(discovery.getSerializer(), "default serializer not set");
        Assert.assertFalse(discovery.getSerializer() instanceof JsonInstanceSerializer, "set serializer is JSON");
    }
}
