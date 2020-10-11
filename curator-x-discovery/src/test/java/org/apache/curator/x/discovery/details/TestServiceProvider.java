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
package org.apache.curator.x.discovery.details;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.Closeable;
import java.util.Collections;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(CuratorTestBase.zk35TestCompatibilityGroup)
public class TestServiceProvider extends BaseClassForTests
{

    @Test
    public void testBasic() throws Exception
    {
        List<Closeable> closeables = Lists.newArrayList();
        try
        {
            CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            closeables.add(client);
            client.start();

            ServiceInstance<String> instance = ServiceInstance.<String>builder().payload("thing").name("test").port(10064).build();
            ServiceDiscovery<String> discovery = ServiceDiscoveryBuilder.builder(String.class).basePath("/test").client(client).thisInstance(instance).build();
            closeables.add(discovery);
            discovery.start();

            ServiceProvider<String> provider = discovery.serviceProviderBuilder().serviceName("test").build();
            closeables.add(provider);
            provider.start();

            assertEquals(provider.getInstance(), instance);

            List<ServiceInstance<String>> list = Lists.newArrayList();
            list.add(instance);
            assertEquals(provider.getAllInstances(), list);
        }
        finally
        {
            Collections.reverse(closeables);
            for ( Closeable c : closeables )
            {
                CloseableUtils.closeQuietly(c);
            }
        }
    }

    @Test
    public void testDisabledInstance() throws Exception
    {
        List<Closeable> closeables = Lists.newArrayList();
        try
        {
            CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            closeables.add(client);
            client.start();

            ServiceInstance<String> instance = ServiceInstance.<String>builder().payload("thing").name("test").port(10064).enabled(false).build();
            InstanceSerializer<String> serializer = new JsonInstanceSerializer<>(String.class, false);
            ServiceDiscovery<String> discovery = ServiceDiscoveryBuilder.builder(String.class).serializer(serializer).basePath("/test").client(client).thisInstance(instance).build();
            closeables.add(discovery);
            discovery.start();

            ServiceProvider<String> provider = discovery.serviceProviderBuilder().serviceName("test").build();
            closeables.add(provider);
            provider.start();

            assertEquals(provider.getInstance(), null);
            assertTrue(provider.getAllInstances().isEmpty(), "Disabled instance still appears available via service provider");
        }
        finally
        {
            Collections.reverse(closeables);
            for ( Closeable c : closeables )
            {
                CloseableUtils.closeQuietly(c);
            }
        }
    }

}
