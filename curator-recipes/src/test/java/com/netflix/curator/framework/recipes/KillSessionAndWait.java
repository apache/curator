/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.curator.framework.recipes;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;
import com.netflix.curator.test.KillSession;
import org.testng.Assert;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class KillSessionAndWait
{
    public static void     kill(CuratorFramework client, String connectionString) throws Exception
    {
        final CountDownLatch        latch = new CountDownLatch(1);
        ConnectionStateListener     listener = new ConnectionStateListener()
        {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState)
            {
                latch.countDown();
            }
        };
        client.getConnectionStateListenable().addListener(listener);
        KillSession.kill(connectionString, client.getZookeeperClient().getZooKeeper().getSessionId(), client.getZookeeperClient().getZooKeeper().getSessionPasswd());
        Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
        client.getConnectionStateListenable().removeListener(listener);
    }
}
