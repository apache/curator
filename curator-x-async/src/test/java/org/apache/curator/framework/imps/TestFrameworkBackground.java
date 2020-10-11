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
package org.apache.curator.framework.imps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class TestFrameworkBackground extends BaseClassForTests
{
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Test
    public void testErrorListener() throws Exception
    {
        //The first call to the ACL provider will return a reasonable
        //value. The second will throw an error. This is because the ACL
        //provider is accessed prior to the backgrounding call.
        final AtomicBoolean aclProviderCalled = new AtomicBoolean(false);
        
        ACLProvider badAclProvider = new ACLProvider()
        {
            @Override
            public List<ACL> getDefaultAcl()
            {
                if(aclProviderCalled.getAndSet(true))
                {
                    throw new UnsupportedOperationException();
                }
                else
                {
                    return new ArrayList<>();
                }
            }

            @Override
            public List<ACL> getAclForPath(String path)
            {
                if(aclProviderCalled.getAndSet(true))
                {
                    throw new UnsupportedOperationException();
                }
                else
                {
                    return new ArrayList<>();
                }
            }
        };
        CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString(server.getConnectString())
            .retryPolicy(new RetryOneTime(1))
            .aclProvider(badAclProvider)
            .build();
        try
        {
            client.start();
            AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);

            final CountDownLatch errorLatch = new CountDownLatch(1);
            UnhandledErrorListener listener = (message, e) -> {
                if ( e instanceof UnsupportedOperationException )
                {
                    errorLatch.countDown();
                }
            };
            async.with(listener).create().forPath("/foo");
            assertTrue(new Timing().awaitLatch(errorLatch));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testListenerConnectedAtStart() throws Exception
    {
        server.stop();

        Timing timing = new Timing(2);
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryNTimes(0, 0));
        try
        {
            client.start();
            AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);

            final CountDownLatch connectedLatch = new CountDownLatch(1);
            final AtomicBoolean firstListenerAction = new AtomicBoolean(true);
            final AtomicReference<ConnectionState> firstListenerState = new AtomicReference<>();
            ConnectionStateListener listener = (client1, newState) ->
            {
                if ( firstListenerAction.compareAndSet(true, false) )
                {
                    firstListenerState.set(newState);
                    System.out.println("First listener state is " + newState);
                }
                if ( newState == ConnectionState.CONNECTED )
                {
                    connectedLatch.countDown();
                }
            };
            client.getConnectionStateListenable().addListener(listener);

            // due to CURATOR-72, this was causing a LOST event to precede the CONNECTED event
            async.create().forPath("/foo");

            server.restart();

            assertTrue(timing.awaitLatch(connectedLatch));
            assertFalse(firstListenerAction.get());
            ConnectionState firstconnectionState = firstListenerState.get();
            assertEquals(firstconnectionState, ConnectionState.CONNECTED, "First listener state MUST BE CONNECTED but is " + firstconnectionState);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testRetries() throws Exception
    {
        final int SLEEP = 1000;
        final int TIMES = 5;

        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryNTimes(TIMES, SLEEP));
        try
        {
            client.start();
            AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);
            client.getZookeeperClient().blockUntilConnectedOrTimedOut();

            final CountDownLatch latch = new CountDownLatch(TIMES);
            final List<Long> times = Lists.newArrayList();
            final AtomicLong start = new AtomicLong(System.currentTimeMillis());
            ((CuratorFrameworkImpl)client).debugListener = data ->
            {
                if ( data.getOperation().getClass().getName().contains("CreateBuilderImpl") )
                {
                    long now = System.currentTimeMillis();
                    times.add(now - start.get());
                    start.set(now);
                    latch.countDown();
                }
            };

            server.stop();
            async.create().forPath("/one");

            latch.await();

            for ( long elapsed : times.subList(1, times.size()) )   // first one isn't a retry
            {
                assertTrue(elapsed >= SLEEP, elapsed + ": " + times);
            }
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    /**
     * Attempt a background operation while Zookeeper server is down.
     * Return code must be {@link org.apache.zookeeper.KeeperException.Code#CONNECTIONLOSS}
     */
    @Test
    public void testCuratorCallbackOnError() throws Exception
    {
        Timing timing = new Timing();
        final CountDownLatch latch = new CountDownLatch(1);
        try ( CuratorFramework client = CuratorFrameworkFactory.builder().connectString(server.getConnectString()).sessionTimeoutMs(timing.session()).connectionTimeoutMs(timing.connection()).retryPolicy(new RetryOneTime(1000)).build() )
        {
            client.start();
            AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);
            // Stop the Zookeeper server
            server.stop();
            // Attempt to retrieve children list
            async.getChildren().forPath("/").handle((children, e) -> {
                if ( e instanceof KeeperException.ConnectionLossException )
                {
                    latch.countDown();
                }
                return null;
            });
            // Check if the callback has been called with a correct return code
            assertTrue(timing.awaitLatch(latch), "Callback has not been called by curator !");
        }
    }

    /**
     * CURATOR-126
     * Shutdown the Curator client while there are still background operations running.
     */
    @Test
    public void testShutdown() throws Exception
    {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory
            .builder()
            .connectString(server.getConnectString())
            .sessionTimeoutMs(timing.session())
            .connectionTimeoutMs(timing.connection()).retryPolicy(new RetryOneTime(1))
            .maxCloseWaitMs(timing.forWaiting().milliseconds())
            .build();
        try
        {
            final AtomicBoolean hadIllegalStateException = new AtomicBoolean(false);
            ((CuratorFrameworkImpl)client).debugUnhandledErrorListener = (message, e) ->
            {
                if ( e instanceof IllegalStateException )
                {
                    hadIllegalStateException.set(true);
                }
            };
            client.start();
            AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);

            final CountDownLatch operationReadyLatch = new CountDownLatch(1);
            ((CuratorFrameworkImpl)client).debugListener = data ->
            {
                try
                {
                    operationReadyLatch.await();
                }
                catch ( InterruptedException e )
                {
                    Thread.currentThread().interrupt();
                }
            };

            // queue a background operation that will block due to the debugListener
            async.create().forPath("/hey");
            timing.sleepABit();

            // close the client while the background is still blocked
            client.close();

            // unblock the background
            operationReadyLatch.countDown();
            timing.sleepABit();

            // should not generate an exception
            assertFalse(hadIllegalStateException.get());
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }
}
