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

package org.apache.curator.framework.recipes.leader;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.BaseClassForTests;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryOneTime;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.internal.annotations.Sets;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TestLeaderSelectorParticipants extends BaseClassForTests
{
    @Test
    public void     testId() throws Exception
    {
        LeaderSelector          selector = null;
        CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            final CountDownLatch        latch = new CountDownLatch(1);
            LeaderSelectorListener      listener = new LeaderSelectorListener()
            {
                @Override
                public void takeLeadership(CuratorFramework client) throws Exception
                {
                    latch.countDown();
                    Thread.currentThread().join();
                }

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                }
            };
            selector = new LeaderSelector(client, "/ls", listener);
            selector.setId("A is A");
            selector.start();

            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));

            Participant leader = selector.getLeader();
            Assert.assertTrue(leader.isLeader());
            Assert.assertEquals(leader.getId(), "A is A");

            Collection<Participant>     participants = selector.getParticipants();
            Assert.assertEquals(participants.size(), 1);
            Assert.assertEquals(participants.iterator().next().getId(), "A is A");
            Assert.assertEquals(participants.iterator().next().getId(), selector.getId());
        }
        finally
        {
            Closeables.closeQuietly(selector);
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void     testBasic() throws Exception
    {
        final int           SELECTOR_QTY = 10;

        List<LeaderSelector>    selectors = Lists.newArrayList();
        CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            final CountDownLatch            leaderLatch = new CountDownLatch(1);
            final CountDownLatch            workingLatch = new CountDownLatch(SELECTOR_QTY);
            LeaderSelectorListener          listener = new LeaderSelectorListener()
            {
                @Override
                public void takeLeadership(CuratorFramework client) throws Exception
                {
                    leaderLatch.countDown();
                    Thread.currentThread().join();
                }

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                }
            };

            for ( int i = 0; i < SELECTOR_QTY; ++i )
            {
                LeaderSelector      selector = new LeaderSelector(client, "/ls", listener)
                {
                    @Override
                    void doWork() throws Exception
                    {
                        workingLatch.countDown();
                        super.doWork();
                    }
                };
                selector.setId(Integer.toString(i));
                selectors.add(selector);
            }

            for ( LeaderSelector selector : selectors )
            {
                selector.start();
            }

            Assert.assertTrue(leaderLatch.await(10, TimeUnit.SECONDS));
            Assert.assertTrue(workingLatch.await(10, TimeUnit.SECONDS));
            
            Thread.sleep(1000); // some time for locks to acquire

            Collection<Participant>     participants = selectors.get(0).getParticipants();
            for ( int i = 1; i < selectors.size(); ++i )
            {
                Assert.assertEquals(participants, selectors.get(i).getParticipants());
            }

            Set<String>                 ids = Sets.newHashSet();
            int                         leaderCount = 0;
            for ( Participant participant : participants )
            {
                if ( participant.isLeader() )
                {
                    ++leaderCount;
                }
                Assert.assertFalse(ids.contains(participant.getId()));
                ids.add(participant.getId());
            }
            Assert.assertEquals(leaderCount, 1);

            Set<String>                 expectedIds = Sets.newHashSet();
            for ( int i = 0; i < SELECTOR_QTY; ++i )
            {
                expectedIds.add(Integer.toString(i));
            }
            Assert.assertEquals(expectedIds, ids);
        }
        finally
        {
            for ( LeaderSelector selector : selectors )
            {
                Closeables.closeQuietly(selector);
            }
            Closeables.closeQuietly(client);
        }
    }
}
