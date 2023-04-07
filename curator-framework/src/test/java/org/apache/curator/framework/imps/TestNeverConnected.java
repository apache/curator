/*
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
import com.google.common.collect.Queues;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.Timing;
import org.junit.jupiter.api.Test;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class TestNeverConnected
{
    @Test
    public void testNeverConnected() throws Exception
    {
        Timing timing = new Timing();

        // use a connection string to a non-existent server
        CuratorFramework client = CuratorFrameworkFactory.newClient("localhost:1111", 100, 100, new RetryOneTime(1));
        try
        {
            final BlockingQueue<ConnectionState> queue = Queues.newLinkedBlockingQueue();
            ConnectionStateListener listener = new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState state)
                {
                    queue.add(state);
                }
            };
            client.getConnectionStateListenable().addListener(listener);
            client.start();

            client.create().inBackground().forPath("/");

            ConnectionState polled = queue.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            assertEquals(polled, ConnectionState.SUSPENDED);
            polled = queue.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            assertEquals(polled, ConnectionState.LOST);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }
}
