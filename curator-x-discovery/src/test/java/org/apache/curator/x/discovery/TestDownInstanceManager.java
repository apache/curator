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

import org.apache.curator.x.discovery.details.ServiceDiscoveryImpl;
import org.apache.curator.x.discovery.details.ServiceProviderImpl;
import org.apache.curator.x.discovery.strategies.RandomStrategy;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.TimeUnit;

public class TestDownInstanceManager
{
    @Test
    public void testBasic() throws Exception
    {
        ServiceInstance<Void> instance1 = ServiceInstance.<Void>builder().name("hey").id("1").build();
        ServiceInstance<Void> instance2 = ServiceInstance.<Void>builder().name("hey").id("2").build();

        DownInstanceManager downInstanceManager = new DownInstanceManager(10, TimeUnit.DAYS);
        Assert.assertFalse(downInstanceManager.hasEntries());
        Assert.assertFalse(downInstanceManager.contains(instance1));
        Assert.assertFalse(downInstanceManager.contains(instance2));

        downInstanceManager.add(instance1);
        Assert.assertTrue(downInstanceManager.contains(instance1));
        Assert.assertFalse(downInstanceManager.contains(instance2));
    }

    @Test
    public void testExpiration() throws Exception
    {
        ServiceInstance<Void> instance1 = ServiceInstance.<Void>builder().name("hey").id("1").build();
        ServiceInstance<Void> instance2 = ServiceInstance.<Void>builder().name("hey").id("2").build();

        DownInstanceManager downInstanceManager = new DownInstanceManager(1, TimeUnit.SECONDS);

        downInstanceManager.add(instance1);
        Assert.assertTrue(downInstanceManager.contains(instance1));
        Assert.assertFalse(downInstanceManager.contains(instance2));

        Thread.sleep(1000);

        Assert.assertFalse(downInstanceManager.contains(instance1));
        Assert.assertFalse(downInstanceManager.contains(instance2));
    }

    @Test
    public void testInProvider() throws Exception
    {
        ServiceInstance<Void> instance1 = ServiceInstance.<Void>builder().name("hey").id("1").build();
        ServiceInstance<Void> instance2 = ServiceInstance.<Void>builder().name("hey").id("2").build();

        DownInstanceManager downInstanceManager = new DownInstanceManager(1, TimeUnit.SECONDS);
        ServiceDiscoveryImpl<Void> discovery = new ServiceDiscoveryImpl<Void>(null, null, null, null);
        ServiceProviderImpl<Void> provider = new ServiceProviderImpl<Void>
        (
            discovery,
            "test",
            new RandomStrategy<Void>(),
            null,
            downInstanceManager
        );
    }
}
