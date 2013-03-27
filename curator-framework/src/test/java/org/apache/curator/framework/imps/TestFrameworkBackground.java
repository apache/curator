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
import com.google.common.io.Closeables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.Timing;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class TestFrameworkBackground extends BaseClassForTests
{
    @Test
    public void         testRetries() throws Exception
    {
        final int SLEEP = 1000;
        final int TIMES = 5;

        Timing                  timing = new Timing();
        CuratorFramework    client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryNTimes(TIMES, SLEEP));
        try
        {
            client.start();
            client.getZookeeperClient().blockUntilConnectedOrTimedOut();

            final CountDownLatch    latch = new CountDownLatch(TIMES);
            final List<Long>        times = Lists.newArrayList();
            final AtomicLong        start = new AtomicLong(System.currentTimeMillis());
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
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void         testBasic() throws Exception
    {
        Timing              timing = new Timing();
        CuratorFramework    client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();

            final CountDownLatch    latch = new CountDownLatch(3);
            final List<String>      paths = Lists.newArrayList();
            BackgroundCallback      callback = new BackgroundCallback()
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    paths.add(event.getPath());
                    latch.countDown();
                }
            };
            client.create().inBackground(callback).forPath("/one");
            client.create().inBackground(callback).forPath("/one/two");
            client.create().inBackground(callback).forPath("/one/two/three");

            latch.await();

            Assert.assertEquals(paths, Arrays.asList("/one", "/one/two", "/one/two/three"));
        }
        finally
        {
            Closeables.closeQuietly(client);
        }
    }
}
