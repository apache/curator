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
package org.apache.curator.framework.recipes.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.TestingServer;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestSimpleDistributedQueue extends BaseClassForTests
{
    private static abstract class QueueUser implements Runnable
    {
        private static final String QUEUE_PATH = "/queue";
        private static final int ITEM_COUNT = 10;

        protected final SimpleDistributedQueue queue;
        private final int sleepMillis;

        public QueueUser(CuratorFramework curator, int sleepMillis)
        {
            this.queue = new SimpleDistributedQueue(curator, QUEUE_PATH);
            this.sleepMillis = sleepMillis;
        }

        @Override
        public void run()
        {
            try
            {
                for ( int i = 0; i < ITEM_COUNT; i++ )
                {
                    processItem(i);
                    Thread.sleep(sleepMillis);
                }
            }
            catch ( Exception e )
            {
                throw new RuntimeException(e);
            }
        }

        protected abstract void processItem(int itemNumber) throws Exception;
    }

    @Test
    public void testHangFromContainerLoss() throws Exception
    {
        // for CURATOR-308

        server.close();
        System.setProperty("znode.container.checkIntervalMs", "100");
        server = new TestingServer();

        Timing timing = new Timing().multiple(.1);
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            ExecutorService executor = Executors.newFixedThreadPool(2);
            executor.execute(new QueueUser(client, timing.milliseconds())
            {
                @Override
                protected void processItem(int itemNumber) throws Exception
                {
                    System.out.println("Offering item");
                    queue.offer(new byte[]{(byte)itemNumber});
                }
            });

            executor.execute(new QueueUser(client, timing.multiple(.5).milliseconds())
            {
                @Override
                protected void processItem(int itemNumber) throws Exception
                {
                    System.out.println("Taking item " + itemNumber);
                    byte[] item = queue.take();
                    if ( item == null )
                    {
                        throw new IllegalStateException("Null result for item " + itemNumber);
                    }
                    System.out.println("Got item " + item[0]);
                }
            });

            executor.shutdown();
            assertTrue(executor.awaitTermination((QueueUser.ITEM_COUNT * 2) * timing.milliseconds(), TimeUnit.MILLISECONDS));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
            System.clearProperty("znode.container.checkIntervalMs");
        }
    }

    @Test
    public void testPollWithTimeout() throws Exception
    {
        CuratorFramework clients[] = null;
        try
        {
            String dir = "/testOffer1";
            final int num_clients = 1;
            clients = new CuratorFramework[num_clients];
            SimpleDistributedQueue queueHandles[] = new SimpleDistributedQueue[num_clients];
            for ( int i = 0; i < clients.length; i++ )
            {
                clients[i] = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
                clients[i].start();
                queueHandles[i] = new SimpleDistributedQueue(clients[i], dir);
            }

            assertNull(queueHandles[0].poll(3, TimeUnit.SECONDS));
        }
        finally
        {
            closeAll(clients);
        }
    }

    @Test
    public void testOffer1() throws Exception
    {
        CuratorFramework clients[] = null;
        try
        {
            String dir = "/testOffer1";
            String testString = "Hello World";
            final int num_clients = 1;
            clients = new CuratorFramework[num_clients];
            SimpleDistributedQueue queueHandles[] = new SimpleDistributedQueue[num_clients];
            for ( int i = 0; i < clients.length; i++ )
            {
                clients[i] = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
                clients[i].start();
                queueHandles[i] = new SimpleDistributedQueue(clients[i], dir);
            }

            queueHandles[0].offer(testString.getBytes());

            byte dequeuedBytes[] = queueHandles[0].remove();
            assertEquals(new String(dequeuedBytes), testString);
        }
        finally
        {
            closeAll(clients);
        }
    }

    @Test
    public void testOffer2() throws Exception
    {
        CuratorFramework clients[] = null;
        try
        {
            String dir = "/testOffer2";
            String testString = "Hello World";
            final int num_clients = 2;
            clients = new CuratorFramework[num_clients];
            SimpleDistributedQueue queueHandles[] = new SimpleDistributedQueue[num_clients];
            for ( int i = 0; i < clients.length; i++ )
            {
                clients[i] = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
                clients[i].start();
                queueHandles[i] = new SimpleDistributedQueue(clients[i], dir);
            }

            queueHandles[0].offer(testString.getBytes());

            byte dequeuedBytes[] = queueHandles[1].remove();
            assertEquals(new String(dequeuedBytes), testString);
        }
        finally
        {
            closeAll(clients);
        }
    }

    @Test
    public void testTake1() throws Exception
    {
        CuratorFramework clients[] = null;
        try
        {
            String dir = "/testTake1";
            String testString = "Hello World";
            final int num_clients = 1;
            clients = new CuratorFramework[num_clients];
            SimpleDistributedQueue queueHandles[] = new SimpleDistributedQueue[num_clients];
            for ( int i = 0; i < clients.length; i++ )
            {
                clients[i] = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
                clients[i].start();
                queueHandles[i] = new SimpleDistributedQueue(clients[i], dir);
            }

            queueHandles[0].offer(testString.getBytes());

            byte dequeuedBytes[] = queueHandles[0].take();
            assertEquals(new String(dequeuedBytes), testString);
        }
        finally
        {
            closeAll(clients);
        }
    }

    @Test
    public void testRemova1() throws Exception
    {
        CuratorFramework clients[] = null;
        try
        {
            String dir = "/testRemove1";
            final int num_clients = 1;
            clients = new CuratorFramework[num_clients];
            SimpleDistributedQueue queueHandles[] = new SimpleDistributedQueue[num_clients];
            for ( int i = 0; i < clients.length; i++ )
            {
                clients[i] = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
                clients[i].start();
                queueHandles[i] = new SimpleDistributedQueue(clients[i], dir);
            }

            try
            {
                queueHandles[0].remove();
            }
            catch ( NoSuchElementException e )
            {
                return;
            }
            assertTrue(false);
        }
        finally
        {
            closeAll(clients);
        }
    }

    public void createNremoveMtest(String dir, int n, int m) throws Exception
    {
        CuratorFramework clients[] = null;
        try
        {
            String testString = "Hello World";
            final int num_clients = 2;
            clients = new CuratorFramework[num_clients];
            SimpleDistributedQueue queueHandles[] = new SimpleDistributedQueue[num_clients];
            for ( int i = 0; i < clients.length; i++ )
            {
                clients[i] = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
                clients[i].start();
                queueHandles[i] = new SimpleDistributedQueue(clients[i], dir);
            }

            for ( int i = 0; i < n; i++ )
            {
                String offerString = testString + i;
                queueHandles[0].offer(offerString.getBytes());
            }

            byte data[] = null;
            for ( int i = 0; i < m; i++ )
            {
                data = queueHandles[1].remove();
            }
            assertEquals(new String(data), testString + (m - 1));
        }
        finally
        {
            closeAll(clients);
        }
    }

    @Test
    public void testRemove2() throws Exception
    {
        createNremoveMtest("/testRemove2", 10, 2);
    }

    @Test
    public void testRemove3() throws Exception
    {
        createNremoveMtest("/testRemove3", 1000, 1000);
    }

    public void createNremoveMelementTest(String dir, int n, int m) throws Exception
    {
        CuratorFramework clients[] = null;
        try
        {
            String testString = "Hello World";
            final int num_clients = 2;
            clients = new CuratorFramework[num_clients];
            SimpleDistributedQueue queueHandles[] = new SimpleDistributedQueue[num_clients];
            for ( int i = 0; i < clients.length; i++ )
            {
                clients[i] = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
                clients[i].start();
                queueHandles[i] = new SimpleDistributedQueue(clients[i], dir);
            }

            for ( int i = 0; i < n; i++ )
            {
                String offerString = testString + i;
                queueHandles[0].offer(offerString.getBytes());
            }

            for ( int i = 0; i < m; i++ )
            {
                queueHandles[1].remove();
            }
            assertEquals(new String(queueHandles[1].element()), testString + m);
        }
        finally
        {
            closeAll(clients);
        }
    }

    @Test
    public void testElement1() throws Exception
    {
        createNremoveMelementTest("/testElement1", 1, 0);
    }

    @Test
    public void testElement2() throws Exception
    {
        createNremoveMelementTest("/testElement2", 10, 2);
    }

    @Test
    public void testElement3() throws Exception
    {
        createNremoveMelementTest("/testElement3", 1000, 500);
    }

    @Test
    public void testElement4() throws Exception
    {
        createNremoveMelementTest("/testElement4", 1000, 1000 - 1);
    }

    @Test
    public void testTakeWait1() throws Exception
    {
        CuratorFramework clients[] = null;
        try
        {
            String dir = "/testTakeWait1";
            final String testString = "Hello World";
            final int num_clients = 1;
            clients = new CuratorFramework[num_clients];
            final SimpleDistributedQueue queueHandles[] = new SimpleDistributedQueue[num_clients];
            for ( int i = 0; i < clients.length; i++ )
            {
                clients[i] = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
                clients[i].start();
                queueHandles[i] = new SimpleDistributedQueue(clients[i], dir);
            }

            final byte[] takeResult[] = new byte[1][];
            Thread takeThread = new Thread()
            {
                public void run()
                {
                    try
                    {
                        takeResult[0] = queueHandles[0].take();
                    }
                    catch ( Exception e )
                    {
                        // ignore
                    }
                }
            };
            takeThread.start();

            Thread.sleep(1000);
            Thread offerThread = new Thread()
            {
                public void run()
                {
                    try
                    {
                        queueHandles[0].offer(testString.getBytes());
                    }
                    catch ( Exception e )
                    {
                        // ignore
                    }
                }
            };
            offerThread.start();
            offerThread.join();

            takeThread.join();

            assertTrue(takeResult[0] != null);
            assertEquals(new String(takeResult[0]), testString);
        }
        finally
        {
            closeAll(clients);
        }
    }

    @Test
    public void testTakeWait2() throws Exception
    {
        String dir = "/testTakeWait2";
        final String testString = "Hello World";
        final int num_clients = 1;
        final CuratorFramework clients[] = new CuratorFramework[num_clients];
        final SimpleDistributedQueue queueHandles[] = new SimpleDistributedQueue[num_clients];
        for ( int i = 0; i < clients.length; i++ )
        {
            clients[i] = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            clients[i].start();
            queueHandles[i] = new SimpleDistributedQueue(clients[i], dir);
        }
        int num_attempts = 2;
        for ( int i = 0; i < num_attempts; i++ )
        {
            final byte[] takeResult[] = new byte[1][];
            final String threadTestString = testString + i;
            Thread takeThread = new Thread()
            {
                public void run()
                {
                    try
                    {
                        takeResult[0] = queueHandles[0].take();
                    }
                    catch ( Exception e )
                    {
                        // ignore
                    }
                }
            };
            takeThread.start();

            Thread.sleep(1000);
            Thread offerThread = new Thread()
            {
                public void run()
                {
                    try
                    {
                        queueHandles[0].offer(threadTestString.getBytes());
                    }
                    catch ( Exception e )
                    {
                        // ignore
                    }
                }
            };
            offerThread.start();
            offerThread.join();

            takeThread.join();

            assertTrue(takeResult[0] != null);
            assertEquals(new String(takeResult[0]), threadTestString);
        }
    }

    private void closeAll(CuratorFramework[] clients)
    {
        if ( clients != null )
        {
            for ( CuratorFramework c : clients )
            {
                CloseableUtils.closeQuietly(c);
            }
        }
    }
}
