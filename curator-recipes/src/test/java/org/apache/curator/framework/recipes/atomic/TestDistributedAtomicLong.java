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
package org.apache.curator.framework.recipes.atomic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.Lists;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryOneTime;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.commons.math.stat.descriptive.SynchronizedSummaryStatistics;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.utils.CloseableUtils;
import org.junit.jupiter.api.Test;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestDistributedAtomicLong extends BaseClassForTests
{
    @Test
    public void     testCorruptedValue() throws Exception
    {
        final CuratorFramework      client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            client.create().forPath("/counter", "foo".getBytes());
            DistributedAtomicLong   dal = new DistributedAtomicLong(client, "/counter", new RetryOneTime(1));
            try
            {
                dal.get().postValue();
            }
            catch ( BufferUnderflowException e )
            {
                fail("", e);
            }
            catch ( BufferOverflowException e )
            {
                fail("", e);
            }
            catch ( RuntimeException e )
            {
                // correct
            }
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void testCompareAndSetWithFreshInstance() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();
            DistributedAtomicLong dal = new DistributedAtomicLong(client, "/counter", new RetryOneTime(1));
            AtomicValue<Long> result = dal.compareAndSet(0L, 1L);
            assertFalse(result.succeeded());

            assertTrue(dal.initialize(0L));
            result = dal.compareAndSet(0L, 1L);
            assertTrue(result.succeeded());

            assertFalse(dal.initialize(0L));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void     testCompareAndSet() throws Exception
    {
        final CuratorFramework      client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            final AtomicBoolean         doIncrement = new AtomicBoolean(false);
            DistributedAtomicLong dal = new DistributedAtomicLong(client, "/counter", new RetryOneTime(1))
            {
                @Override
                public byte[] valueToBytes(Long newValue)
                {
                    if ( doIncrement.get() )
                    {
                        DistributedAtomicLong inc = new DistributedAtomicLong(client, "/counter", new RetryOneTime(1));
                        try
                        {
                            // this will force a bad version exception
                            inc.increment();
                        }
                        catch ( Exception e )
                        {
                            throw new Error(e);
                        }
                    }

                    return super.valueToBytes(newValue);
                }
            };
            dal.forceSet(1L);

            assertTrue(dal.compareAndSet(1L, 5L).succeeded());
            assertFalse(dal.compareAndSet(1L, 5L).succeeded());

            doIncrement.set(true);
            assertFalse(dal.compareAndSet(5L, 10L).succeeded());
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testForceSet() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            final DistributedAtomicLong dal = new DistributedAtomicLong(client, "/counter", new RetryOneTime(1));

            ExecutorService                 executorService = Executors.newFixedThreadPool(2);
            executorService.submit
            (
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        for ( int i = 0; i < 1000; ++i )
                        {
                            dal.increment();
                            Thread.sleep(10);
                        }
                        return null;
                    }
                }
            );
            executorService.submit
            (
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        for ( int i = 0; i < 1000; ++i )
                        {
                            dal.forceSet(0L);
                            Thread.sleep(10);
                        }
                        return null;
                    }
                }
            );

            assertTrue(dal.get().preValue() < 10);
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testSimulation() throws Exception
    {
        final int           threadQty = 20;
        final int           executionQty = 50;

        final AtomicInteger optimisticTries = new AtomicInteger();
        final AtomicInteger promotedLockTries = new AtomicInteger();
        final AtomicInteger failures = new AtomicInteger();
        final AtomicInteger errors = new AtomicInteger();

        final SummaryStatistics timingStats = new SynchronizedSummaryStatistics();
        List<Future<Void>>      procs = Lists.newArrayList();
        ExecutorService         executorService = Executors.newFixedThreadPool(threadQty);
        for ( int i = 0; i < threadQty; ++i )
        {
            Callable<Void>          proc = new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    doSimulation(executionQty, timingStats, optimisticTries, promotedLockTries, failures, errors);
                    return null;
                }
            };
            procs.add(executorService.submit(proc));
        }

        for ( Future<Void> f : procs )
        {
            f.get();
        }

        System.out.println("OptimisticTries: " + optimisticTries.get());
        System.out.println("PromotedLockTries: " + promotedLockTries.get());
        System.out.println("Failures: " + failures.get());
        System.out.println("Errors: " + errors.get());
        System.out.println();

        System.out.println("Avg time: " + timingStats.getMean());
        System.out.println("Max time: " + timingStats.getMax());
        System.out.println("Min time: " + timingStats.getMin());
        System.out.println("Qty: " + timingStats.getN());

        assertEquals(errors.get(), 0);
        assertTrue(optimisticTries.get() > 0);
        assertTrue(promotedLockTries.get() > 0);
    }

    @Test
    public void     testBasic() throws Exception
    {
        CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            DistributedAtomicLong dal = new DistributedAtomicLong(client, "/foo/bar/counter", new RetryOneTime(1));

            AtomicValue<Long>           value = dal.increment();
            assertTrue(value.succeeded());
            assertEquals(value.getStats().getOptimisticTries(), 1);
            assertEquals(value.getStats().getPromotedLockTries(), 0);
            assertEquals(value.preValue().longValue(), 0L);
            assertEquals(value.postValue().longValue(), 1L);

            value = dal.decrement();
            assertTrue(value.succeeded());
            assertEquals(value.getStats().getOptimisticTries(), 1);
            assertEquals(value.getStats().getPromotedLockTries(), 0);
            assertEquals(value.preValue().longValue(), 1L);
            assertEquals(value.postValue().longValue(), 0L);

            value = dal.add(10L);
            assertTrue(value.succeeded());
            assertEquals(value.getStats().getOptimisticTries(), 1);
            assertEquals(value.getStats().getPromotedLockTries(), 0);
            assertEquals(value.preValue().longValue(), 0L);
            assertEquals(value.postValue().longValue(), 10L);

            value = dal.subtract(5L);
            assertTrue(value.succeeded());
            assertEquals(value.getStats().getOptimisticTries(), 1);
            assertEquals(value.getStats().getPromotedLockTries(), 0);
            assertEquals(value.preValue().longValue(), 10L);
            assertEquals(value.postValue().longValue(), 5L);
        }
        finally
        {
            client.close();
        }
    }

    private void doSimulation(int executionQty, SummaryStatistics timingStats, AtomicInteger optimisticTries, AtomicInteger promotedLockTries, AtomicInteger failures, AtomicInteger errors) throws Exception
    {
        Random              random = new Random();
        long                previousValue = -1;
        CuratorFramework    client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            RetryPolicy             retryPolicy = new ExponentialBackoffRetry(3, 3);
            PromotedToLock.Builder  builder = PromotedToLock.builder().lockPath("/lock").retryPolicy(retryPolicy);

            DistributedAtomicLong dal = new DistributedAtomicLong(client, "/counter", retryPolicy, builder.build());
            for ( int i = 0; i < executionQty; ++i )
            {
                Thread.sleep(random.nextInt(10));

                long                start = System.currentTimeMillis();
                AtomicValue<Long>   value = dal.increment();
                long                elapsed = System.currentTimeMillis() - start;
                timingStats.addValue(elapsed);

                if ( value.succeeded() )
                {
                    if ( value.postValue() <= previousValue )
                    {
                        errors.incrementAndGet();
                    }

                    previousValue = value.postValue();
                }
                else
                {
                    failures.incrementAndGet();
                }

                optimisticTries.addAndGet(value.getStats().getOptimisticTries());
                promotedLockTries.addAndGet(value.getStats().getPromotedLockTries());
            }
        }
        finally
        {
            client.close();
        }
    }
}
