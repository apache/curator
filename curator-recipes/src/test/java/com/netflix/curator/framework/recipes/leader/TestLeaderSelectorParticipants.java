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

package com.netflix.curator.framework.recipes.leader;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.BaseClassForTests;
import com.netflix.curator.retry.RetryOneTime;
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
                public void notifyClientClosing(CuratorFramework client)
                {
                }

                @Override
                public void unhandledError(CuratorFramework client, Throwable e)
                {
                }
            };
            selector = new LeaderSelector(client, "/ls", listener);
            selector.setId("A is A");
            selector.start();

            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));

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
                public void notifyClientClosing(CuratorFramework client)
                {
                }

                @Override
                public void unhandledError(CuratorFramework client, Throwable e)
                {
                }
            };

            for ( int i = 0; i < SELECTOR_QTY; ++i )
            {
                LeaderSelector      selector = new LeaderSelector(client, "/ls", listener);
                selector.setId(Integer.toString(i));
                selectors.add(selector);
            }

            for ( LeaderSelector selector : selectors )
            {
                selector.start();
            }
            Thread.sleep(1000); // give some time to get going

            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));

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
