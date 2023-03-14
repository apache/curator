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

package org.apache.curator.framework.recipes.leader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.curator.test.compatibility.Timing2;
import org.apache.curator.utils.CloseableUtils;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;

public class TestLeaderLatchCluster extends CuratorTestBase
{
    private static final int MAX_LOOPS = 5;

    private static class ClientAndLatch
    {
        final CuratorFramework      client;
        final LeaderLatch           latch;
        final int                   index;

        private ClientAndLatch(CuratorFramework client, LeaderLatch latch, int index)
        {
            this.client = client;
            this.latch = latch;
            this.index = index;
        }
    }

    @Test
    public void testInCluster() throws Exception
    {
        final int PARTICIPANT_QTY = 3;
        final int sessionLength = timing.session() / 4;

        List<ClientAndLatch>    clients = Lists.newArrayList();
        TestingCluster          cluster = createAndStartCluster(PARTICIPANT_QTY);
        try
        {
            List<InstanceSpec>      instances = Lists.newArrayList(cluster.getInstances());
            for ( int i = 0; i < PARTICIPANT_QTY; ++i )
            {
                CuratorFramework        client = CuratorFrameworkFactory.newClient(instances.get(i).getConnectString(), sessionLength, sessionLength, new ExponentialBackoffRetry(100, 3));
                LeaderLatch             latch = new LeaderLatch(client, "/latch");

                clients.add(new ClientAndLatch(client, latch, i));
                client.start();
                latch.start();
            }

            ClientAndLatch leader = waitForALeader(clients, timing);
            assertNotNull(leader);

            cluster.killServer(instances.get(leader.index));

            Thread.sleep(sessionLength * 2);

            leader = waitForALeader(clients, timing);
            assertNotNull(leader);

            assertEquals(getLeaders(clients).size(), 1);
        }
        finally
        {
            for ( ClientAndLatch client : clients )
            {
                CloseableUtils.closeQuietly(client.latch);
                CloseableUtils.closeQuietly(client.client);
            }
            CloseableUtils.closeQuietly(cluster);
        }
    }

    @Override
    protected void createServer()
    {
        // NOP
    }

    private ClientAndLatch waitForALeader(List<ClientAndLatch> latches, Timing2 timing) throws InterruptedException
    {
        for ( int i = 0; i < MAX_LOOPS; ++i )
        {
            List<ClientAndLatch> leaders = getLeaders(latches);
            if ( leaders.size() != 0 )
            {
                return leaders.get(0);
            }
            timing.sleepABit();
        }
        return null;
    }

    private List<ClientAndLatch> getLeaders(Collection<ClientAndLatch> latches)
    {
        List<ClientAndLatch> leaders = Lists.newArrayList();
        for ( ClientAndLatch clientAndLatch : latches )
        {
            if ( clientAndLatch.latch.hasLeadership() )
            {
                leaders.add(clientAndLatch);
            }
        }
        return leaders;
    }
}
