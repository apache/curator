/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.curator.framework.recipes.locks;

import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.BaseClassForTests;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.test.Timing;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Random;

public class TestChildReaper extends BaseClassForTests
{
    @Test
    public void     testSomeNodes() throws Exception
    {

        Timing                  timing = new Timing();
        ChildReaper             reaper = null;
        CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();

            Random              r = new Random();
            int                 nonEmptyNodes = 0;
            for ( int i = 0; i < 10; ++i )
            {
                client.create().creatingParentsIfNeeded().forPath("/test/" + Integer.toString(i));
                if ( r.nextBoolean() )
                {
                    client.create().forPath("/test/" + Integer.toString(i) + "/foo");
                    ++nonEmptyNodes;
                }
            }

            reaper = new ChildReaper(client, "/test", Reaper.Mode.REAP_UNTIL_DELETE, 1);
            reaper.start();

            timing.forWaiting().sleepABit();

            Stat    stat = client.checkExists().forPath("/test");
            Assert.assertEquals(stat.getNumChildren(), nonEmptyNodes);
        }
        finally
        {
            Closeables.closeQuietly(reaper);
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void     testSimple() throws Exception
    {
        Timing                  timing = new Timing();
        ChildReaper             reaper = null;
        CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();

            for ( int i = 0; i < 10; ++i )
            {
                client.create().creatingParentsIfNeeded().forPath("/test/" + Integer.toString(i));
            }

            reaper = new ChildReaper(client, "/test", Reaper.Mode.REAP_UNTIL_DELETE, 1);
            reaper.start();

            timing.forWaiting().sleepABit();

            Stat    stat = client.checkExists().forPath("/test");
            Assert.assertEquals(stat.getNumChildren(), 0);
        }
        finally
        {
            Closeables.closeQuietly(reaper);
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void     testNamespace() throws Exception
    {
        Timing                  timing = new Timing();
        ChildReaper             reaper = null;
        CuratorFramework        client = CuratorFrameworkFactory.builder()
            .connectString(server.getConnectString())
            .sessionTimeoutMs(timing.session())
            .connectionTimeoutMs(timing.connection())
            .retryPolicy(new RetryOneTime(1))
            .namespace("foo")
            .build();
        try
        {
            client.start();

            for ( int i = 0; i < 10; ++i )
            {
                client.create().creatingParentsIfNeeded().forPath("/test/" + Integer.toString(i));
            }

            reaper = new ChildReaper(client, "/test", Reaper.Mode.REAP_UNTIL_DELETE, 1);
            reaper.start();

            timing.forWaiting().sleepABit();

            Stat    stat = client.checkExists().forPath("/test");
            Assert.assertEquals(stat.getNumChildren(), 0);

            stat = client.usingNamespace(null).checkExists().forPath("/foo/test");
            Assert.assertNotNull(stat);
            Assert.assertEquals(stat.getNumChildren(), 0);
        }
        finally
        {
            Closeables.closeQuietly(reaper);
            Closeables.closeQuietly(client);
        }
    }
}
