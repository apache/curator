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

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.KeeperException.Code;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class TestFrameworkBackground extends BaseClassForTests
{
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Test
    public void testListenerConnectedAtStart() throws Exception
    {
        server.stop();

        Timing timing = new Timing(2);
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryNTimes(0, 0));
        try
        {
            client.start();

            final CountDownLatch connectedLatch = new CountDownLatch(1);
            final AtomicBoolean firstListenerAction = new AtomicBoolean(true);
            final AtomicReference<ConnectionState> firstListenerState = new AtomicReference<ConnectionState>();
            ConnectionStateListener listener = new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
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
                }
            };
            client.getConnectionStateListenable().addListener(listener);

            // due to CURATOR-72, this was causing a LOST event to precede the CONNECTED event
            client.create().inBackground().forPath("/foo");

            server.restart();

            Assert.assertTrue(timing.awaitLatch(connectedLatch));
            Assert.assertFalse(firstListenerAction.get());
            ConnectionState firstconnectionState = firstListenerState.get();
            Assert.assertEquals(firstconnectionState, ConnectionState.CONNECTED, "First listener state MUST BE CONNECTED but is " + firstconnectionState);
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
            client.getZookeeperClient().blockUntilConnectedOrTimedOut();

            final CountDownLatch latch = new CountDownLatch(TIMES);
            final List<Long> times = Lists.newArrayList();
            final AtomicLong start = new AtomicLong(System.currentTimeMillis());
            ((CuratorFrameworkImpl)client).debugListener = new CuratorFrameworkImpl.DebugBackgroundListener()
            {
                @Override
                public void listen(OperationAndData<?> data)
                {
                    if ( data.getOperation().getClass().getName().contains("CreateBuilderImpl") )
                    {
                        long now = System.currentTimeMillis();
                        times.add(now - start.get());
                        start.set(now);
                        latch.countDown();
                    }
                }
            };

            server.stop();
            client.create().inBackground().forPath("/one");

            latch.await();

            for ( long elapsed : times.subList(1, times.size()) )   // first one isn't a retry
            {
                Assert.assertTrue(elapsed >= SLEEP, elapsed + ": " + times);
            }
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testBasic() throws Exception
    {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();

            final BlockingQueue<String> paths = Queues.newLinkedBlockingQueue();
                BackgroundCallback callback = new BackgroundCallback()
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    paths.add(event.getPath());
                }
            };
            client.create().inBackground(callback).forPath("/one");
            Assert.assertEquals(paths.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), "/one");
            client.create().inBackground(callback).forPath("/one/two");
            Assert.assertEquals(paths.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), "/one/two");
            client.create().inBackground(callback).forPath("/one/two/three");
            Assert.assertEquals(paths.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), "/one/two/three");
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    /**
     * Attempt a background operation while Zookeeper server is down.
     * Return code must be {@link Code#CONNECTIONLOSS}
     */
    @Test
    public void testCuratorCallbackOnError() throws Exception
    {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString(server.getConnectString()).sessionTimeoutMs(timing.session()).connectionTimeoutMs(timing.connection()).retryPolicy(new RetryOneTime(1000)).build();
        final CountDownLatch latch = new CountDownLatch(1);
        try
        {
            client.start();
            BackgroundCallback curatorCallback = new BackgroundCallback()
            {

                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    if ( event.getResultCode() == Code.CONNECTIONLOSS.intValue() )
                    {
                        latch.countDown();
                    }
                }
            };
            // Stop the Zookeeper server
            server.stop();
            // Attempt to retrieve children list
            client.getChildren().inBackground(curatorCallback).forPath("/");
            // Check if the callback has been called with a correct return code
            Assert.assertTrue(timing.awaitLatch(latch), "Callback has not been called by curator !");
        }
        finally
        {
            client.close();
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
            ((CuratorFrameworkImpl)client).debugUnhandledErrorListener = new UnhandledErrorListener()
            {
                @Override
                public void unhandledError(String message, Throwable e)
                {
                    if ( e instanceof IllegalStateException )
                    {
                        hadIllegalStateException.set(true);
                    }
                }
            };
            client.start();

            final CountDownLatch operationReadyLatch = new CountDownLatch(1);
            ((CuratorFrameworkImpl)client).debugListener = new CuratorFrameworkImpl.DebugBackgroundListener()
            {
                @Override
                public void listen(OperationAndData<?> data)
                {
                    try
                    {
                        operationReadyLatch.await();
                    }
                    catch ( InterruptedException e )
                    {
                        Thread.currentThread().interrupt();
                    }
                }
            };

            // queue a background operation that will block due to the debugListener
            client.create().inBackground().forPath("/hey");
            timing.sleepABit();

            // close the client while the background is still blocked
            client.close();

            // unblock the background
            operationReadyLatch.countDown();
            timing.sleepABit();

            // should not generate an exception
            Assert.assertFalse(hadIllegalStateException.get());
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }


}
