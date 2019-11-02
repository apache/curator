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
package org.apache.curator.framework.state;

import org.apache.curator.connection.StandardConnectionHandlingPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.compatibility.Timing2;
import org.apache.curator.test.compatibility.Zk35MethodInterceptor;
import org.apache.curator.utils.CloseableUtils;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Test(groups = Zk35MethodInterceptor.curatorV2Group)
public class TestConnectionStateManager extends BaseClassForTests {

    @Test
    public void testSessionConnectionStateErrorPolicyWithExpirationPercent30() throws Exception {
        Timing2 timing = new Timing2();
        CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString(server.getConnectString())
            .connectionTimeoutMs(1000)
            .sessionTimeoutMs(timing.session())
            .retryPolicy(new RetryOneTime(1))
            .connectionStateErrorPolicy(new SessionConnectionStateErrorPolicy())
            .connectionHandlingPolicy(new StandardConnectionHandlingPolicy(30))
            .build();

        // we should get LOST around 30% of a session plus a little "slop" for processing, etc.
        final int lostStateExpectedMs = (timing.session() / 3) + timing.forSleepingABit().milliseconds();
        try
        {
            CountDownLatch connectedLatch = new CountDownLatch(1);
            CountDownLatch lostLatch = new CountDownLatch(1);
            ConnectionStateListener stateListener = new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    if ( newState == ConnectionState.CONNECTED )
                    {
                        connectedLatch.countDown();
                    }
                    if ( newState == ConnectionState.LOST )
                    {
                        lostLatch.countDown();
                    }
                }
            };

            timing.sleepABit();

            client.getConnectionStateListenable().addListener(stateListener);
            client.start();
            Assert.assertTrue(timing.awaitLatch(connectedLatch));
            server.close();

            Assert.assertTrue(lostLatch.await(lostStateExpectedMs, TimeUnit.MILLISECONDS));
        }
        finally {
            CloseableUtils.closeQuietly(client);
        }
    }
}
