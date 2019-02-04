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

import com.google.common.collect.Queues;
import org.apache.curator.connection.StandardConnectionHandlingPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.compatibility.Timing2;
import org.apache.curator.utils.CloseableUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

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

        try {
            final BlockingQueue<String> states = Queues.newLinkedBlockingQueue();
            ConnectionStateListener stateListener = new ConnectionStateListener() {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState) {
                states.add(newState.name());
                }
            };

            timing.sleepABit();

            client.getConnectionStateListenable().addListener(stateListener);
            client.start();
            Assert.assertEquals(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.CONNECTED.name());
            server.close();

            Assert.assertEquals(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.SUSPENDED.name());
            Assert.assertEquals(states.poll(timing.session() / 3, TimeUnit.MILLISECONDS), ConnectionState.LOST.name());
        }
        finally {
            CloseableUtils.closeQuietly(client);
        }
    }
}
