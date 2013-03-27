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

import com.google.common.io.Closeables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.zookeeper.Watcher;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TestMultiClient extends BaseClassForTests
{
    @Test
    public void     testNotify() throws Exception
    {
        CuratorFramework client1 = null;
        CuratorFramework client2 = null;
        try
        {
            client1 = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            client2 = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));

            client1.start();
            client2.start();

            final CountDownLatch        latch = new CountDownLatch(1);
            client1.getCuratorListenable().addListener
            (
                new CuratorListener()
                {
                    @Override
                    public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception
                    {
                        if ( event.getType() == CuratorEventType.WATCHED )
                        {
                            if ( event.getWatchedEvent().getType() == Watcher.Event.EventType.NodeDataChanged )
                            {
                                if ( event.getPath().equals("/test") )
                                {
                                    latch.countDown();
                                }
                            }
                        }
                    }
                }
            );

            client1.create().forPath("/test", new byte[]{1, 2, 3});
            client1.checkExists().watched().forPath("/test");

            client2.getCuratorListenable().addListener
            (
                new CuratorListener()
                {
                    @Override
                    public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception
                    {
                        if ( event.getType() == CuratorEventType.SYNC )
                        {
                            client.setData().forPath("/test", new byte[]{10, 20});
                        }
                    }
                }
            );

            client2.sync("/test", null);

            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
        }
        finally
        {
            Closeables.closeQuietly(client1);
            Closeables.closeQuietly(client2);
        }
    }
}
