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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.BackgroundPathAndBytesable;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.SetDataBuilder;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.Test;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestSetData extends BaseClassForTests
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
     * Tests normal setData operations
     */
    @Test
    public void testNormal() throws Exception
    {
        CuratorFramework client = createClient();
        try
        {
            client.start();

            Stat stat = new Stat();

            String path = "/test";
            byte[] data = new byte[] {1};
            byte[] data2 = new byte[] {1, 2};

            check(client, client.setData(), path, data, NONODE.intValue(), -1);

            client.create().forPath(path, createData);

            check(client, client.setData(), path, data, OK.intValue(), 1);

            // version should be 1 at this point, so test withVersion
            check(client, client.setData().withVersion(1), path, data2, OK.intValue(), 2);

            // check that set fails with version mismatch
            check(client, client.setData().withVersion(1), path, data, BADVERSION.intValue(), 2);

            // check that failed request didn't change znode
            assertArrayEquals(data2, client.getData().forPath(path));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    /**
     * Tests background versions of setData
     */
    @Test
    public void testBackground() throws Exception
    {
        CuratorFramework client = createClient();
        try
        {
            client.start();

            Stat stat = new Stat();

            String path = "/test";
            byte[] data = new byte[] {1};
            byte[] data2 = new byte[] {1, 2};

            // check setData fails when node doesn't exist
            checkBackground(client, client.setData(), path, data, NONODE.intValue(), -1);

            client.create().forPath(path, createData);

            checkBackground(client, client.setData(), path, data, OK.intValue(), 1);

            // version should be 1 at this point
            checkBackground(client, client.setData().withVersion(1), path, data2, OK.intValue(), 2);

            // check setData fails when version mismatches
            checkBackground(client, client.setData().withVersion(1), path, data, BADVERSION.intValue(), 3);

            // make sure old data is still correct
            assertArrayEquals(data2, client.getData().forPath(path));
 
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    private void check(CuratorFramework client, BackgroundPathAndBytesable<Stat> builder, String path, byte[] data, int expectedCode, int expectedVersionAfter) throws Exception
    {
        try
        {
            builder.forPath(path, data);
            assertEquals(expectedCode, OK.intValue());
            Stat stat = new Stat();
            byte[] actualData = client.getData().storingStatIn(stat).forPath(path);
            assertTrue(IdempotentUtils.matches(expectedVersionAfter, data, stat.getVersion(), actualData));
        }
        catch (KeeperException e)
        {
            assertEquals(expectedCode, e.getCode());
        }
    }

    private void checkBackground(CuratorFramework client, BackgroundPathAndBytesable<Stat> builder, String path, byte[] data, int expectedCode, int expectedVersionAfter) throws Exception
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

        builder.inBackground(callback).forPath(path, data);

        assertTrue(latch.await(5000, TimeUnit.MILLISECONDS), "Callback not invoked");
        assertEquals(expectedCode, actualCode.get());

        if (expectedCode == OK.intValue())
        {
            Stat stat = new Stat();
            byte[] actualData = client.getData().storingStatIn(stat).forPath(path);
            assertTrue(IdempotentUtils.matches(expectedVersionAfter, data, stat.getVersion(), actualData));
        }
    }

    /**
     * Tests all cases of idempotent set
     */
    @Test
    public void testIdempotentSet() throws Exception
    {
        CuratorFramework client = createClient();
        try
        {
            client.start();

            Stat stat = new Stat();

            String path = "/idpset";
            String pathBack = "/idpsetback";
            byte[] data1 = new byte[] {1, 2, 3};
            byte[] data2 = new byte[] {4, 5, 6};
            byte[] data3 = new byte[] {7, 8, 9};

            // check foreground and background

            // set when node doesn't exist should fail
            check(client, client.setData().idempotent(), path, data1, NONODE.intValue(), -1);
            checkBackground(client, client.setData().idempotent(), pathBack, data1, NONODE.intValue(), -1);

            client.create().forPath(path, createData);
            client.create().forPath(pathBack, createData);

            // check normal set succeeds
            check(client, client.setData().idempotent(), path, data1, OK.intValue(), 1);
            checkBackground(client, client.setData().idempotent(), pathBack, data1, OK.intValue(), 1);

            // check normal set with version succeeds
            check(client, client.setData().idempotent().withVersion(1), path, data2, OK.intValue(), 2);
            checkBackground(client, client.setData().idempotent().withVersion(1), pathBack, data2, OK.intValue(), 2);

            // repeating the same op should succeed without updating the version
            check(client, client.setData().idempotent().withVersion(1), path, data2, OK.intValue(), 2);
            checkBackground(client, client.setData().idempotent().withVersion(1), pathBack, data2, OK.intValue(), 2);

            // same op with different data should fail even though version matches
            check(client, client.setData().idempotent().withVersion(1), path, data3, BADVERSION.intValue(), 1);
            checkBackground(client, client.setData().idempotent().withVersion(1), pathBack, data3, BADVERSION.intValue(), 1);

            // same op with same data should fail even because version mismatches
            check(client, client.setData().idempotent().withVersion(0), path, data2, BADVERSION.intValue(), 2);
            checkBackground(client, client.setData().idempotent().withVersion(0), pathBack, data2, BADVERSION.intValue(), 2);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    private SetDataBuilder clBefore(SetDataBuilder builder)
    {
        ((SetDataBuilderImpl)builder).failBeforeNextSetForTesting = true;
        return builder;
    }

    private SetDataBuilder clAfter(SetDataBuilder builder)
    {
        ((SetDataBuilderImpl)builder).failNextSetForTesting = true;
        return builder;
    }

    private SetDataBuilder clCheck(SetDataBuilder builder)
    {
        ((SetDataBuilderImpl)builder).failNextIdempotentCheckForTesting = true;
        return builder;
    }

    // Test that idempotent set automatically retries correctly upon connectionLoss
    @Test
    public void testIdempotentSetConnectionLoss() throws Exception {
        CuratorFramework client = createClient();
        try
        {
            client.start();
            String idpPath = "/idpset";
            String idpPathBack = "/idpsetback";
            String path = "/set";
            String pathBack = "/setBack";
            byte[] data = new byte[] {1};
            byte[] data2 = new byte[] {1, 2};
            byte[] data3 = new byte[] {1, 2, 3};
            byte[] data4 = new byte[] {1, 2, 3, 4};

            // check foreground and background
            // test that set fails when node doesn't exist with CL before or after
            check(client, clBefore(client.setData().idempotent()), idpPath, data, NONODE.intValue(), -1);
            checkBackground(client, clBefore(client.setData().idempotent()), idpPathBack, data, NONODE.intValue(), -1);
            check(client, clAfter(client.setData().idempotent()), idpPath, data, NONODE.intValue(), -1);
            checkBackground(client, clAfter(client.setData().idempotent()), idpPathBack, data, NONODE.intValue(), -1);
            check(client, clCheck(client.setData().idempotent()), idpPath, data, NONODE.intValue(), -1);
            checkBackground(client, clCheck(client.setData().idempotent()), idpPathBack, data, NONODE.intValue(), -1);

            // create nodes
            client.create().forPath(idpPath, createData);
            client.create().forPath(idpPathBack, createData);

            // test that idempotent set succeeds with connection loss before or after first set.
            // the version should only go up in all cases except CL after, without a version specified, then it should up 2

            check(client, clBefore(client.setData().idempotent()).withVersion(0), idpPath, data, OK.intValue(), 1);
            checkBackground(client, clBefore(client.setData().idempotent()).withVersion(0), idpPathBack, data, OK.intValue(), 1);
            check(client, clAfter(client.setData().idempotent()).withVersion(1), idpPath, data2, OK.intValue(), 2);
            checkBackground(client, clAfter(client.setData().idempotent()).withVersion(1), idpPathBack, data2, OK.intValue(), 2);

            check(client, clBefore(client.setData().idempotent()), idpPath, data3, OK.intValue(), 3);
            checkBackground(client, clBefore(client.setData().idempotent()), idpPathBack, data3, OK.intValue(), 3);
            check(client, clAfter(client.setData().idempotent()), idpPath, data4, OK.intValue(), 5);
            checkBackground(client, clAfter(client.setData().idempotent()), idpPathBack, data4, OK.intValue(), 5);

            // test that idempotency succeeds when node is already in desired final state withVersion(4) -> (version 5, data4)
            check(client, clBefore(client.setData().idempotent()).withVersion(4), idpPath, data4, OK.intValue(), 5);
            checkBackground(client, clBefore(client.setData().idempotent()).withVersion(4), idpPathBack, data4, OK.intValue(), 5);
            check(client, clAfter(client.setData().idempotent()).withVersion(4), idpPath, data4, OK.intValue(), 5);
            checkBackground(client, clAfter(client.setData().idempotent()).withVersion(4), idpPathBack, data4, OK.intValue(), 5);
            check(client, clCheck(client.setData().idempotent()).withVersion(4), idpPath, data4, OK.intValue(), 5);
            checkBackground(client, clCheck(client.setData().idempotent()).withVersion(4), idpPathBack, data4, OK.intValue(), 5); 

            // test that idempotent set fails correctly when withVersion is set, before or after connectionloss, if data or version does not match
            // version is wrong (0, would have to be 4) but data is right (data4)
            check(client, clBefore(client.setData().idempotent()).withVersion(0), idpPath, data4, BADVERSION.intValue(), -1);
            checkBackground(client, clBefore(client.setData().idempotent()).withVersion(0), idpPathBack, data4, BADVERSION.intValue(), -1);
            check(client, clAfter(client.setData().idempotent()).withVersion(0), idpPath, data4, BADVERSION.intValue(), -1);
            checkBackground(client, clAfter(client.setData().idempotent()).withVersion(0), idpPathBack, data4, BADVERSION.intValue(), -1);
            check(client, clCheck(client.setData().idempotent()).withVersion(0), idpPath, data4, BADVERSION.intValue(), -1);
            checkBackground(client, clCheck(client.setData().idempotent()).withVersion(0), idpPathBack, data4, BADVERSION.intValue(), -1);

            // version is right (4) but data is wrong (data, would have to be data4)
            check(client, clBefore(client.setData().idempotent()).withVersion(4), idpPath, data, BADVERSION.intValue(), -1);
            checkBackground(client, clBefore(client.setData().idempotent()).withVersion(4), idpPathBack, data, BADVERSION.intValue(), -1);
            check(client, clAfter(client.setData().idempotent()).withVersion(4), idpPath, data, BADVERSION.intValue(), -1);
            checkBackground(client, clAfter(client.setData().idempotent()).withVersion(4), idpPathBack, data, BADVERSION.intValue(), -1);
            check(client, clCheck(client.setData().idempotent()).withVersion(4), idpPath, data, BADVERSION.intValue(), -1);
            checkBackground(client, clCheck(client.setData().idempotent()).withVersion(4), idpPathBack, data, BADVERSION.intValue(), -1);


            // test that non-idempotent set withVersion succeeds when retrying with connectionloss before, but fails with connectionloss after
            client.create().forPath(path, createData);
            client.create().forPath(pathBack, createData);

            check(client, clBefore(client.setData()).withVersion(0), path, data, OK.intValue(), 1);
            checkBackground(client, clBefore(client.setData()).withVersion(0), pathBack, data, OK.intValue(), 1);
            check(client, clAfter(client.setData()).withVersion(1), path, data2, BADVERSION.intValue(), -1);
            checkBackground(client, clAfter(client.setData()).withVersion(1), pathBack, data2, BADVERSION.intValue(), -1);

        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }
}
