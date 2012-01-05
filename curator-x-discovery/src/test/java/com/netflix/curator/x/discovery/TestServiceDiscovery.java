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

package com.netflix.curator.x.discovery;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.test.KillSession;
import com.netflix.curator.test.TestingServer;
import com.netflix.curator.x.discovery.details.JsonInstanceSerializer;
import com.netflix.curator.x.discovery.details.ServiceDiscoveryImpl;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.Closeable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class TestServiceDiscovery
{
    @Test
    public void         testCrashedInstance() throws Exception
    {
        List<Closeable>     closeables = Lists.newArrayList();
        TestingServer       server = new TestingServer();
        closeables.add(server);
        try
        {
            final int TIMEOUT_SECONDS = 5;

            CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), TIMEOUT_SECONDS * 1000, TIMEOUT_SECONDS * 1000, new RetryOneTime(1));
            closeables.add(client);
            client.start();

            ServiceInstance<String>     instance = ServiceInstance.<String>builder().payload("thing").name("test").port(10064).build();
            ServiceDiscovery<String>    discovery = new ServiceDiscoveryImpl<String>(client, "/test", new JsonInstanceSerializer<String>(String.class), instance);
            closeables.add(discovery);
            discovery.start();

            Assert.assertEquals(discovery.queryForInstances("test").size(), 1);
            
            KillSession.kill(client.getZookeeperClient().getZooKeeper(), server.getConnectString());

            Assert.assertEquals(discovery.queryForInstances("test").size(), 0);
        }
        finally
        {
            Collections.reverse(closeables);
            for ( Closeable c : closeables )
            {
                Closeables.closeQuietly(c);
            }
        }
    }

    @Test
    public void         testMultipleInstances() throws Exception
    {
        final String        SERVICE_ONE = "one";
        final String        SERVICE_TWO = "two";

        List<Closeable>     closeables = Lists.newArrayList();
        TestingServer       server = new TestingServer();
        closeables.add(server);
        try
        {
            CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            closeables.add(client);
            client.start();

            ServiceInstance<Void>       s1_i1 = ServiceInstance.<Void>builder().name(SERVICE_ONE).build();
            ServiceInstance<Void>       s1_i2 = ServiceInstance.<Void>builder().name(SERVICE_ONE).build();
            ServiceInstance<Void>       s2_i1 = ServiceInstance.<Void>builder().name(SERVICE_TWO).build();
            ServiceInstance<Void>       s2_i2 = ServiceInstance.<Void>builder().name(SERVICE_TWO).build();

            ServiceDiscovery<Void>      discovery = ServiceDiscoveryBuilder.builder(Void.class).client(client).basePath("/test").build();
            closeables.add(discovery);
            discovery.start();

            discovery.registerService(s1_i1);
            discovery.registerService(s1_i2);
            discovery.registerService(s2_i1);
            discovery.registerService(s2_i2);

            Assert.assertEquals(Sets.newHashSet(discovery.queryForNames()), Sets.newHashSet(SERVICE_ONE, SERVICE_TWO));

            Collection<ServiceInstance<Void>> list = Sets.newHashSet();
            list.add(s1_i1);
            list.add(s1_i2);
            Assert.assertEquals(Sets.<ServiceInstance<Void>>newHashSet(discovery.queryForInstances(SERVICE_ONE)), list);

            list.clear();

            list.add(s2_i1);
            list.add(s2_i2);
            Assert.assertEquals(Sets.<ServiceInstance<Void>>newHashSet(discovery.queryForInstances(SERVICE_TWO)), list);
        }
        finally
        {
            Collections.reverse(closeables);
            for ( Closeable c : closeables )
            {
                Closeables.closeQuietly(c);
            }
        }
    }

    @Test
    public void         testBasic() throws Exception
    {
        List<Closeable>     closeables = Lists.newArrayList();
        TestingServer       server = new TestingServer();
        closeables.add(server);
        try
        {
            CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            closeables.add(client);
            client.start();
            
            ServiceInstance<String>     instance = ServiceInstance.<String>builder().payload("thing").name("test").port(10064).build();
            ServiceDiscovery<String>    discovery = ServiceDiscoveryBuilder.builder(String.class).basePath("/test").client(client).thisInstance(instance).build();
            closeables.add(discovery);
            discovery.start();

            Assert.assertEquals(discovery.queryForNames(), Arrays.asList("test"));

            List<ServiceInstance<String>> list = Lists.newArrayList();
            list.add(instance);
            Assert.assertEquals(discovery.queryForInstances("test"), list);
        }
        finally
        {
            Collections.reverse(closeables);
            for ( Closeable c : closeables )
            {
                Closeables.closeQuietly(c);
            }
        }
    }
}
