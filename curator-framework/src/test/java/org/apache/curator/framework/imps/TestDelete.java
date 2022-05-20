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

import static org.apache.zookeeper.KeeperException.Code.*;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.BackgroundPathable;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.curator.framework.api.DeleteBuilderMain;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Test;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class TestDelete extends BaseClassForTests
{

    private static byte[] createData = new byte[] {5, 6, 7, 8};

    private CuratorFramework createClient()
    {
        return CuratorFrameworkFactory.builder().
            connectString(server.getConnectString()).
            retryPolicy(new RetryOneTime(1)).
            build();
    }

    /**
     * Tests normal delete operations
     */
    @Test
    public void testNormal() throws Exception
    {
        CuratorFramework client = createClient();
        try
        {
            client.start();

            String path = "/test";

            // test delete with no node fails
            check(client, client.delete(), path, NONODE.intValue());

            client.create().forPath(path, createData);
            // test delete with no version succeeds
            check(client, client.delete(), path, OK.intValue());

            client.create().forPath(path, createData);
            // test fails with wrong verion
            check(client, client.delete().withVersion(1), path, BADVERSION.intValue());
 
            // test succeeds with correct version
            check(client, client.delete().withVersion(0), path, OK.intValue());

            client.create().forPath(path, createData);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    /**
     * Tests background versions of delete
     */
    @Test
    public void testBackground() throws Exception
    {
        CuratorFramework client = createClient();
        try
        {
            client.start();

            String path = "/test";

            // test delete with no node fails
            checkBackground(client, client.delete(), path, NONODE.intValue());
            
            client.create().forPath(path, createData); 
            // test delete with no version succeeds
            checkBackground(client, client.delete(), path, OK.intValue());
            
            client.create().forPath(path, createData);
            // test fails with wrong verion
            checkBackground(client, client.delete().withVersion(1), path, BADVERSION.intValue());
            
            // test succeeds with correct version
            checkBackground(client, client.delete().withVersion(0), path, OK.intValue());

        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }


    // TODO jslocum refactor these into common idempotent test utils?
    private void checkBackground(CuratorFramework client, BackgroundPathable<Void> builder, String path, int expectedCode) throws Exception
    {
        AtomicInteger actualCode = new AtomicInteger(-1);
        CountDownLatch latch = new CountDownLatch(1);

        BackgroundCallback callback = new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
            {
                actualCode.set(event.getResultCode());
                latch.countDown();
            }
        };

        builder.inBackground(callback).forPath(path);

        assertTrue(latch.await(5000, TimeUnit.MILLISECONDS), "Callback not invoked");
        assertEquals(expectedCode, actualCode.get());

        if ( expectedCode == OK.intValue() )
        {
            assertNull(client.checkExists().forPath(path));
        }
    }

    private void check(CuratorFramework client, BackgroundPathable<Void> builder, String path, int expectedCode) throws Exception
    {
        try
        {
            builder.forPath(path);
            assertEquals(expectedCode, OK.intValue());
            assertNull(client.checkExists().forPath(path));
        }
        catch (KeeperException e)
        {
            assertEquals(expectedCode, e.getCode());
        }
    }

    /**
     * Tests all cases of idempotent delete
     */
    @Test
    public void testIdempotentDelete() throws Exception
    {
        CuratorFramework client = createClient();
        try
        {
            client.start();

            String path = "/idpset";
            String pathBack = "/idpsetback";

            // check foreground and background

            // delete idempotent when node doesn't exist should succeed, with or without version
            check(client, client.delete().idempotent().withVersion(0), path, OK.intValue());
            checkBackground(client, client.delete().idempotent().withVersion(0), pathBack, OK.intValue());
            check(client, client.delete().idempotent(), path, OK.intValue());
            checkBackground(client, client.delete().idempotent(), pathBack, OK.intValue());

            client.create().forPath(path, createData);
            client.create().forPath(pathBack, createData);

            // check normal delete without version succeeds
            check(client, client.delete().idempotent(), path, OK.intValue());
            checkBackground(client, client.delete().idempotent(), pathBack, OK.intValue());

            client.create().forPath(path, createData);
            client.create().forPath(pathBack, createData);

            // check normal delete with version succeeds
            check(client, client.delete().idempotent().withVersion(0), path, OK.intValue());
            checkBackground(client, client.delete().idempotent().withVersion(0), pathBack, OK.intValue());

            // create nodes, update version to 1 with setData
            client.create().forPath(path, createData);
            client.setData().forPath(path, createData);
            client.create().forPath(pathBack, createData);
            client.setData().forPath(pathBack, createData);

            // check that idempotent delete fails with wrong version
            check(client, client.delete().idempotent().withVersion(0), path, BADVERSION.intValue());
            checkBackground(client, client.delete().idempotent().withVersion(0), pathBack, BADVERSION.intValue());
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    private DeleteBuilderMain clBefore(DeleteBuilderMain builder)
    {
        ((DeleteBuilderImpl)builder).failBeforeNextDeleteForTesting = true;
        return builder;
    }

    private DeleteBuilderMain clAfter(DeleteBuilderMain builder)
    {
        ((DeleteBuilderImpl)builder).failNextDeleteForTesting = true;
        return builder;
    }

    // Test that idempotent set automatically retries correctly upon connectionLoss
    @Test
    public void testIdempotentDeleteConnectionLoss() throws Exception {
        CuratorFramework client = createClient();
        try
        {
            client.start();
            String path = "/delete";
            String pathBack = "/deleteBack";
            String pathNormal = "/deleteNormal";
            String pathNormalBack = "/deleteNormalBack";

            // test that connection loss before or after, version or no version, idempotent delete succeeds if node doesn't exist
            check(client, clBefore(client.delete().idempotent()).withVersion(0), path, OK.intValue());
            checkBackground(client, clBefore(client.delete().idempotent()).withVersion(0), pathBack, OK.intValue());
            check(client, clBefore(client.delete().idempotent()), path, OK.intValue());
            checkBackground(client, clBefore(client.delete().idempotent()), pathBack, OK.intValue());

            check(client, clAfter(client.delete().idempotent()).withVersion(0), path, OK.intValue());
            checkBackground(client, clAfter(client.delete().idempotent()).withVersion(0), pathBack, OK.intValue());
            check(client, clBefore(client.delete().idempotent()), path, OK.intValue());
            checkBackground(client, clBefore(client.delete().idempotent()), pathBack, OK.intValue());

            // test that connection loss before or after, correct version or no version, idempotent delete succeeds
            client.create().forPath(path, createData);
            client.create().forPath(pathBack, createData);
            check(client, clBefore(client.delete().idempotent()), path, OK.intValue());
            checkBackground(client, clBefore(client.delete().idempotent()), pathBack, OK.intValue());

            client.create().forPath(path, createData);
            client.create().forPath(pathBack, createData);
            check(client, clBefore(client.delete().idempotent()).withVersion(0), path, OK.intValue());
            checkBackground(client, clBefore(client.delete().idempotent()).withVersion(0), pathBack, OK.intValue());

            client.create().forPath(path, createData);
            client.create().forPath(pathBack, createData);
            check(client, clAfter(client.delete().idempotent()), path, OK.intValue());
            checkBackground(client, clAfter(client.delete().idempotent()), pathBack, OK.intValue());

            client.create().forPath(path, createData);
            client.create().forPath(pathBack, createData);
            check(client, clAfter(client.delete().idempotent()).withVersion(0), path, OK.intValue());
            checkBackground(client, clAfter(client.delete().idempotent()).withVersion(0), pathBack, OK.intValue());

            // test that connection loss, before or after, delete with version fails on wrong version
            client.create().forPath(path, createData);
            client.create().forPath(pathBack, createData);
            check(client, clBefore(client.delete().idempotent()).withVersion(2), path, BADVERSION.intValue());
            checkBackground(client, clBefore(client.delete().idempotent()).withVersion(2), pathBack, BADVERSION.intValue());

            check(client, clAfter(client.delete().idempotent()).withVersion(2), path, BADVERSION.intValue());
            checkBackground(client, clAfter(client.delete().idempotent()).withVersion(2), pathBack, BADVERSION.intValue());

            // test that non-idempotent delete with or without version succeeds when retrying with connectionloss before
            client.create().forPath(pathNormal, createData);
            client.create().forPath(pathNormalBack, createData);
            check(client, clBefore(client.delete()).withVersion(0), pathNormal, OK.intValue());
            checkBackground(client, clBefore(client.delete()).withVersion(0), pathNormalBack, OK.intValue());

            client.create().forPath(pathNormal, createData);
            client.create().forPath(pathNormalBack, createData);
            check(client, clBefore(client.delete()), pathNormal, OK.intValue());
            checkBackground(client, clBefore(client.delete()), pathNormalBack, OK.intValue());

            // but fails with connectionloss after with NONODE because the previous call actually deleted it
            client.create().forPath(pathNormal, createData);
            client.create().forPath(pathNormalBack, createData);
            check(client, clAfter(client.delete()).withVersion(0), pathNormal, NONODE.intValue());
            checkBackground(client, clAfter(client.delete()).withVersion(0), pathNormalBack, NONODE.intValue());

            client.create().forPath(pathNormal, createData);
            client.create().forPath(pathNormalBack, createData);
            check(client, clAfter(client.delete()), pathNormal, NONODE.intValue());
            checkBackground(client, clAfter(client.delete()), pathNormalBack, NONODE.intValue()); 
  
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testQuietDelete() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            client.delete().quietly().forPath("/foo/bar");

            final BlockingQueue<Integer> rc = new LinkedBlockingQueue<>();
            BackgroundCallback backgroundCallback = new BackgroundCallback()
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    rc.add(event.getResultCode());
                }
            };
            client.delete().quietly().inBackground(backgroundCallback).forPath("/foo/bar/hey");

            Integer code = rc.poll(new Timing().milliseconds(), TimeUnit.MILLISECONDS);
            assertNotNull(code);
            assertEquals(code.intValue(), OK.intValue());
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testBackgroundDelete() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            client.getCuratorListenable().addListener
                (
                    new CuratorListener()
                    {
                        @Override
                        public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception
                        {
                            if ( event.getType() == CuratorEventType.DELETE )
                            {
                                assertEquals(event.getPath(), "/head");
                                ((CountDownLatch)event.getContext()).countDown();
                            }
                        }
                    }
                );

            client.create().forPath("/head");
            assertNotNull(client.checkExists().forPath("/head"));

            CountDownLatch latch = new CountDownLatch(1);
            client.delete().inBackground(latch).forPath("/head");
            assertTrue(latch.await(10, TimeUnit.SECONDS));
            assertNull(client.checkExists().forPath("/head"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testBackgroundDeleteWithChildren() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            client.getCuratorListenable().addListener
                (
                    new CuratorListener()
                    {
                        @Override
                        public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception
                        {
                            if ( event.getType() == CuratorEventType.DELETE )
                            {
                                assertEquals(event.getPath(), "/one/two");
                                ((CountDownLatch)event.getContext()).countDown();
                            }
                        }
                    }
                );

            client.create().creatingParentsIfNeeded().forPath("/one/two/three/four");
            assertNotNull(client.checkExists().forPath("/one/two/three/four"));

            CountDownLatch latch = new CountDownLatch(1);
            client.delete().deletingChildrenIfNeeded().inBackground(latch).forPath("/one/two");
            assertTrue(latch.await(10, TimeUnit.SECONDS));
            assertNull(client.checkExists().forPath("/one/two"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testDelete() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            client.create().forPath("/head");
            assertNotNull(client.checkExists().forPath("/head"));
            client.delete().forPath("/head");
            assertNull(client.checkExists().forPath("/head"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testDeleteWithChildren() throws Exception
    {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1)).build();
        client.start();
        try
        {
            client.create().creatingParentsIfNeeded().forPath("/one/two/three/four/five/six", "foo".getBytes());
            client.delete().deletingChildrenIfNeeded().forPath("/one/two/three/four/five");
            assertNull(client.checkExists().forPath("/one/two/three/four/five"));
            client.delete().deletingChildrenIfNeeded().forPath("/one/two");
            assertNull(client.checkExists().forPath("/one/two"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testDeleteGuaranteedWithChildren() throws Exception
    {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1)).build();
        client.start();
        try
        {
            client.create().creatingParentsIfNeeded().forPath("/one/two/three/four/five/six", "foo".getBytes());
            client.delete().guaranteed().deletingChildrenIfNeeded().forPath("/one/two/three/four/five");
            assertNull(client.checkExists().forPath("/one/two/three/four/five"));
            client.delete().guaranteed().deletingChildrenIfNeeded().forPath("/one/two");
            assertNull(client.checkExists().forPath("/one/two"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

}
