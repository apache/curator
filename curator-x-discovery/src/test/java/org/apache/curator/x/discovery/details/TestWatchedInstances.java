/*
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestWatchedInstances extends BaseClassForTests
{
    @Test
    public void testWatchedInstances() throws Exception
    {
        Timing timing = new Timing();
        List<Closeable> closeables = Lists.newArrayList();
        try
        {
            CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            closeables.add(client);
            client.start();

            ServiceInstance<String> instance = ServiceInstance.<String>builder().payload("thing").name("test").port(10064).build();
            ServiceDiscovery<String> discovery = ServiceDiscoveryBuilder
                .builder(String.class)
                .basePath("/test")
                .client(client)
                .thisInstance(instance)
                .watchInstances(true)
                .build();
            closeables.add(discovery);
            discovery.start();

            assertEquals(discovery.queryForNames(), Arrays.asList("test"));

            List<ServiceInstance<String>> list = Lists.newArrayList();
            list.add(instance);
            assertEquals(discovery.queryForInstances("test"), list);

            ServiceDiscoveryImpl<String> discoveryImpl = (ServiceDiscoveryImpl<String>)discovery;
            ServiceInstance<String> changedInstance = ServiceInstance.<String>builder()
                .id(instance.getId())
                .address(instance.getAddress())
                .payload("different")
                .name(instance.getName())
                .port(instance.getPort())
                .build();
            String path = discoveryImpl.pathForInstance("test", instance.getId());
            byte[] bytes = discoveryImpl.getSerializer().serialize(changedInstance);
            client.setData().forPath(path, bytes);
            timing.sleepABit();

            ServiceInstance<String> registeredService = discoveryImpl.getRegisteredService(instance.getId());
            assertNotNull(registeredService);
            assertEquals(registeredService.getPayload(), "different");
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
