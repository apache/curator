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

import com.google.common.collect.Lists;
import org.apache.curator.x.discovery.details.InstanceProvider;
import org.apache.curator.x.discovery.strategies.RandomStrategy;
import org.apache.curator.x.discovery.strategies.RoundRobinStrategy;
import org.apache.curator.x.discovery.strategies.StickyStrategy;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.List;

public class TestStrategies
{
    private static class TestInstanceProvider implements InstanceProvider<Void>
    {
        private final List<ServiceInstance<Void>>       instances;

        private TestInstanceProvider(int qty) throws Exception
        {
            this(qty, 0);
        }

        private TestInstanceProvider(int qty, int startingAt) throws Exception
        {
            instances = Lists.newArrayList();
            for ( int i = 0; i < qty; ++i )
            {
                ServiceInstanceBuilder<Void> builder = ServiceInstance.builder();
                instances.add(builder.id(Integer.toString(i + startingAt)).name("foo").build());
            }
        }

        @Override
        public List<ServiceInstance<Void>> getInstances() throws Exception
        {
            return instances;
        }
    }

    @Test
    public void     testRandom() throws Exception
    {
        final int                       QTY = 10;
        final int                       ITERATIONS = 1000;

        TestInstanceProvider            instanceProvider = new TestInstanceProvider(QTY, 0);
        ProviderStrategy<Void>          strategy = new RandomStrategy<Void>();

        long[]                          counts = new long[QTY];
        for ( int i = 0; i < ITERATIONS; ++i )
        {
            ServiceInstance<Void>   instance = strategy.getInstance(instanceProvider);
            int                     id = Integer.parseInt(instance.getId());
            counts[id]++;
        }

        SummaryStatistics               statistic = new SummaryStatistics();
        for ( int i = 0; i < QTY; ++i )
        {
            statistic.addValue(counts[i]);
        }
        Assert.assertTrue(statistic.getStandardDeviation() <= (QTY * 2), "" + statistic.getStandardDeviation()); // meager check for even distribution
    }

    @Test
    public void     testRoundRobin() throws Exception
    {
        final int                       QTY = 10;

        TestInstanceProvider            instanceProvider = new TestInstanceProvider(QTY);
        ProviderStrategy<Void>          strategy = new RoundRobinStrategy<Void>();

        for ( int i = 0; i < QTY; ++i )
        {
            ServiceInstance<Void> instance = strategy.getInstance(instanceProvider);
            Assert.assertEquals(instance.getId(), Integer.toString(i));
        }

        for ( int i = 0; i < (1234 * QTY); ++i )
        {
            ServiceInstance<Void> instance = strategy.getInstance(instanceProvider);
            Assert.assertEquals(instance.getId(), Integer.toString(i % QTY));
        }
    }

    @Test
    public void     testSticky() throws Exception
    {
        final int                       QTY = 10;

        TestInstanceProvider            instanceProvider = new TestInstanceProvider(QTY);
        StickyStrategy<Void>            strategy = new StickyStrategy<Void>(new RandomStrategy<Void>());

        ServiceInstance<Void>           theInstance = strategy.getInstance(instanceProvider);
        int                             instanceNumber = strategy.getInstanceNumber();
        for ( int i = 0; i < 1000; ++i )
        {
            Assert.assertEquals(strategy.getInstance(instanceProvider), theInstance);
        }

        // assert what happens when an instance goes down
        instanceProvider = new TestInstanceProvider(QTY, QTY);
        Assert.assertFalse(strategy.getInstance(instanceProvider).equals(theInstance));
        Assert.assertFalse(instanceNumber == strategy.getInstanceNumber());

        theInstance = strategy.getInstance(instanceProvider);
        for ( int i = 0; i < 1000; ++i )
        {
            Assert.assertEquals(strategy.getInstance(instanceProvider), theInstance);
        }
    }
}
