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
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.strategies.RandomStrategy;

public class ServiceCacheLeakTester
{
    public static void main(String[] args) throws Exception
    {
        TestingServer testingServer = new TestingServer();

        final CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(testingServer.getConnectString(), new RetryOneTime(1));
        try
        {
            curatorFramework.start();

            doWork(curatorFramework);
            System.gc();

            System.out.println("Done - get dump");
            Thread.currentThread().join();
        }
        finally
        {
            CloseableUtils.closeQuietly(curatorFramework);
            CloseableUtils.closeQuietly(testingServer);
        }
    }

    private static void doWork(CuratorFramework curatorFramework) throws Exception
    {
        ServiceInstance<Void> thisInstance = ServiceInstance.<Void>builder().name("myservice").build();
        final ServiceDiscovery<Void> serviceDiscovery = ServiceDiscoveryBuilder.builder(Void.class).client(curatorFramework.usingNamespace("dev")).basePath("/instances").thisInstance(thisInstance).build();
        serviceDiscovery.start();

        for ( int i = 0; i < 100000; i++ )
        {
            final ServiceProvider<Void> s = serviceProvider(serviceDiscovery, "myservice");
            s.start();
            try
            {
                s.getInstance().buildUriSpec();
            }
            finally
            {
                s.close();
            }
        }
    }

    private static ServiceProvider<Void> serviceProvider(ServiceDiscovery<Void> serviceDiscovery, String name) throws Exception
    {
        return serviceDiscovery.serviceProviderBuilder().serviceName(name).providerStrategy(new RandomStrategy<Void>()).build();
    }
}