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
package org.apache.curator.x.rpc;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.generated.*;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ThreadUtils;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class RpcTests extends BaseClassForTests
{
    private Timing timing = new Timing();
    private CuratorProjectionServer thriftServer;
    private CuratorService.Client curatorServiceClient;
    private EventService.Client eventServiceClient;
    private int thriftPort;

    @BeforeMethod
    @Override
    public void setup() throws Exception
    {
        super.setup();

        ObjectMapper mapper = new ObjectMapper();

        ObjectNode connectionNode = mapper.createObjectNode();
        connectionNode.put("name", "test");
        connectionNode.put("connectionString", server.getConnectString());

        ObjectNode thriftNode = mapper.createObjectNode();
        thriftPort = InstanceSpec.getRandomPort();
        thriftNode.put("port", thriftPort);

        ArrayNode connections = mapper.createArrayNode();
        connections.add(connectionNode);

        ObjectNode node = mapper.createObjectNode();
        node.put("connections", connections);
        node.put("thrift", thriftNode);

        final String configurationJson = mapper.writeValueAsString(node);

        thriftServer = CuratorProjectionServer.startServer(configurationJson);

        TSocket clientTransport = new TSocket("localhost", thriftPort);
        clientTransport.setTimeout(timing.connection());
        clientTransport.open();
        TProtocol clientProtocol = new TBinaryProtocol(clientTransport);
        curatorServiceClient = new CuratorService.Client(clientProtocol);

        TSocket eventTransport = new TSocket("localhost", thriftPort);
        eventTransport.setTimeout(timing.connection());
        eventTransport.open();
        TProtocol eventProtocol = new TBinaryProtocol(eventTransport);
        eventServiceClient = new EventService.Client(eventProtocol);

    }

    @AfterMethod
    @Override
    public void teardown() throws Exception
    {
        thriftServer.stop();

        super.teardown();
    }

    @Test
    public void testBasic() throws Exception
    {
        CuratorProjection curatorProjection = curatorServiceClient.newCuratorProjection("test");
        CreateSpec spec = new CreateSpec();
        spec.path = "/test";
        spec.data = ByteBuffer.wrap("value".getBytes());
        OptionalPath node = curatorServiceClient.createNode(curatorProjection, spec);
        Assert.assertEquals(node.path, "/test");

        GetDataSpec dataSpec = new GetDataSpec();
        dataSpec.path = "/test";
        OptionalData data = curatorServiceClient.getData(curatorProjection, dataSpec);
        Assert.assertEquals(data.data, ByteBuffer.wrap("value".getBytes()));
    }

    @Test
    public void testEvents() throws Exception
    {
        final CuratorProjection curatorProjection = curatorServiceClient.newCuratorProjection("test");

        final CountDownLatch connectedLatch = new CountDownLatch(1);
        final CountDownLatch nodeCreatedLatch = new CountDownLatch(1);
        Callable<Void> proc = new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                while ( !Thread.currentThread().isInterrupted() )
                {
                    CuratorEvent event = eventServiceClient.getNextEvent(curatorProjection);
                    if ( event.type == CuratorEventType.CONNECTION_CONNECTED )
                    {
                        connectedLatch.countDown();
                    }
                    else if ( event.type == CuratorEventType.WATCHED )
                    {
                        if ( event.watchedEvent.eventType == EventType.NodeCreated )
                        {
                            nodeCreatedLatch.countDown();
                        }
                    }
                }
                return null;
            }
        };
        Future<Void> eventFuture = ThreadUtils.newSingleThreadExecutor("test").submit(proc);

        Assert.assertTrue(timing.awaitLatch(connectedLatch));

        ExistsSpec spec = new ExistsSpec();
        spec.path = "/test";
        spec.watched = true;
        curatorServiceClient.exists(curatorProjection, spec);

        CreateSpec createSpec = new CreateSpec();
        createSpec.path = "/test";
        curatorServiceClient.createNode(curatorProjection, createSpec);

        Assert.assertTrue(timing.awaitLatch(nodeCreatedLatch));

        eventFuture.cancel(true);
    }

    @Test
    public void testLockMultiThread() throws Exception
    {
        final Timing timing = new Timing();

        TSocket clientTransport = new TSocket("localhost", thriftPort);
        clientTransport.setTimeout(timing.connection());
        clientTransport.open();
        TProtocol clientProtocol = new TBinaryProtocol(clientTransport);
        final CuratorService.Client secondCuratorServiceClient = new CuratorService.Client(clientProtocol);
        ExecutorService service = ThreadUtils.newFixedThreadPool(2, "test");
        ExecutorCompletionService<Void> completer = new ExecutorCompletionService<Void>(service);

        final CountDownLatch lockLatch = new CountDownLatch(2);
        final AtomicBoolean hasTheLock = new AtomicBoolean();
        for ( int i = 0; i < 2; ++i )
        {
            final CuratorService.Client client = (i == 0) ? curatorServiceClient : secondCuratorServiceClient;
            Callable<Void> proc = new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    CuratorProjection curatorProjection = client.newCuratorProjection("test");
                    LockProjection lockProjection = client.acquireLock(curatorProjection, "/lock", timing.forWaiting().milliseconds());
                    if ( lockProjection.id == null )
                    {
                        throw new Exception("Could not acquire lock");
                    }
                    try
                    {
                        if ( !hasTheLock.compareAndSet(false, true) )
                        {
                            throw new Exception("Two lockers");
                        }

                        timing.sleepABit();
                    }
                    finally
                    {
                        hasTheLock.set(false);
                        lockLatch.countDown();
                        client.closeGenericProjection(curatorProjection, lockProjection.id);
                    }

                    return null;
                }
            };
            completer.submit(proc);
        }

        completer.take().get();
        completer.take().get();

        Assert.assertTrue(timing.awaitLatch(lockLatch));

        service.shutdownNow();
    }

    @Test
    public void testRecoverableException() throws Exception
    {
        CuratorProjection curatorProjection = curatorServiceClient.newCuratorProjection("test");
        CreateSpec spec = new CreateSpec();
        spec.path = "/this/wont/work";
        spec.data = ByteBuffer.wrap("value".getBytes());
        try
        {
            curatorServiceClient.createNode(curatorProjection, spec);
            Assert.fail("Should have failed");
        }
        catch ( CuratorException e )
        {
            Assert.assertEquals(e.getType(), ExceptionType.NODE);
            Assert.assertNotNull(e.nodeException);
            Assert.assertEquals(e.nodeException, NodeExceptionType.NONODE);
        }
    }

    @Test
    public void testEphemeralCleanup() throws Exception
    {
        CuratorProjection curatorProjection = curatorServiceClient.newCuratorProjection("test");
        CreateSpec spec = new CreateSpec();
        spec.path = "/test";
        spec.data = ByteBuffer.wrap("value".getBytes());
        spec.mode = CreateMode.EPHEMERAL;
        OptionalPath node = curatorServiceClient.createNode(curatorProjection, spec);
        System.out.println(node);

        final CountDownLatch latch = new CountDownLatch(1);
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();
            Watcher watcher = new Watcher()
            {
                @Override
                public void process(WatchedEvent event)
                {
                    if ( event.getType() == Event.EventType.NodeDeleted )
                    {
                        latch.countDown();
                    }
                }
            };
            client.checkExists().usingWatcher(watcher).forPath("/test");

            curatorServiceClient.closeCuratorProjection(curatorProjection);

            Assert.assertTrue(timing.awaitLatch(latch));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }
}
