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
package org.apache.curator.framework.recipes.leader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryOneTime;
import org.junit.jupiter.api.Test;

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

            assertTrue(latch.await(10, TimeUnit.SECONDS));

            Participant leader = selector.getLeader();
            assertTrue(leader.isLeader());
            assertEquals(leader.getId(), "A is A");

            Collection<Participant>     participants = selector.getParticipants();
            assertEquals(participants.size(), 1);
            assertEquals(participants.iterator().next().getId(), "A is A");
            assertEquals(participants.iterator().next().getId(), selector.getId());
        }
        finally
        {
            CloseableUtils.closeQuietly(selector);
            CloseableUtils.closeQuietly(client);
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

            assertTrue(leaderLatch.await(10, TimeUnit.SECONDS));
            assertTrue(workingLatch.await(10, TimeUnit.SECONDS));
            
            Thread.sleep(1000); // some time for locks to acquire

            Collection<Participant>     participants = selectors.get(0).getParticipants();
            for ( int i = 1; i < selectors.size(); ++i )
            {
                assertEquals(participants, selectors.get(i).getParticipants());
            }

            Set<String>                 ids = Sets.newHashSet();
            int                         leaderCount = 0;
            for ( Participant participant : participants )
            {
                if ( participant.isLeader() )
                {
                    ++leaderCount;
                }
                assertFalse(ids.contains(participant.getId()));
                ids.add(participant.getId());
            }
            assertEquals(leaderCount, 1);

            Set<String>                 expectedIds = Sets.newHashSet();
            for ( int i = 0; i < SELECTOR_QTY; ++i )
            {
                expectedIds.add(Integer.toString(i));
            }
            assertEquals(expectedIds, ids);
        }
        finally
        {
            for ( LeaderSelector selector : selectors )
            {
                CloseableUtils.closeQuietly(selector);
            }
            CloseableUtils.closeQuietly(client);
        }
    }
}
