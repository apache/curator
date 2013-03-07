/*
 * Copyright 2013 Netflix, Inc.
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

package org.apache.curator.framework.recipes.leader;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.Timing;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Collection;
import java.util.List;

public class TestLeaderLatchCluster
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

        List<ClientAndLatch>    clients = Lists.newArrayList();
        Timing                  timing = new Timing();
        TestingCluster          cluster = new TestingCluster(PARTICIPANT_QTY);
        try
        {
            cluster.start();

            List<InstanceSpec>      instances = Lists.newArrayList(cluster.getInstances());
            for ( int i = 0; i < PARTICIPANT_QTY; ++i )
            {
                CuratorFramework        client = CuratorFrameworkFactory.newClient(instances.get(i).getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
                LeaderLatch             latch = new LeaderLatch(client, "/latch");

                clients.add(new ClientAndLatch(client, latch, i));
                client.start();
                latch.start();
            }

            ClientAndLatch leader = waitForALeader(clients, timing);
            Assert.assertNotNull(leader);

            cluster.killServer(instances.get(leader.index));

            Thread.sleep(timing.multiple(2).session());

            leader = waitForALeader(clients, timing);
            Assert.assertNotNull(leader);

            Assert.assertEquals(getLeaders(clients).size(), 1);
        }
        finally
        {
            for ( ClientAndLatch client : clients )
            {
                Closeables.closeQuietly(client.latch);
                Closeables.closeQuietly(client.client);
            }
            Closeables.closeQuietly(cluster);
        }
    }

    private ClientAndLatch waitForALeader(List<ClientAndLatch> latches, Timing timing) throws InterruptedException
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
