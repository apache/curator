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
package org.apache.curator.framework.recipes.nodes;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;

public class TestGroupMemberListener extends BaseClassForTests
{
    @Test
    public void testBasic() throws Exception
    {
        Timing timing = new Timing();
        GroupMember groupMember1 = null;
        GroupMember groupMember2 = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            groupMember1 = new GroupMember(client, "/member", "1");
            groupMember1.start();

            final CountDownLatch memberAddedLatch = new CountDownLatch(1);
            final CountDownLatch memberLeftLatch = new CountDownLatch(1);
            final CountDownLatch memberUpdatedLatch = new CountDownLatch(1);
            GroupMemberListener listener = new GroupMemberListener()
            {
                @Override
                public void groupEvent(CuratorFramework client, GroupMemberEvent event) throws Exception
                {
                    if (event.getType() == GroupMemberEvent.Type.MEMBER_JOINED)
                    {
                        memberAddedLatch.countDown();
                    }
                    else if (event.getType() == GroupMemberEvent.Type.MEMBER_LEFT)
                    {
                        memberLeftLatch.countDown();
                    }
                    else if (event.getType() == GroupMemberEvent.Type.MEMBER_UPDATED)
                    {
                        memberUpdatedLatch.countDown();
                    }
                }
            };
            groupMember1.getListenable().addListener(listener);

            groupMember2 = new GroupMember(client, "/member", "2");
            groupMember2.start();
            timing.sleepABit();
            Assert.assertTrue(timing.awaitLatch(memberAddedLatch));
            groupMember2.setThisData("new data".getBytes());
            timing.sleepABit();
            Assert.assertTrue(timing.awaitLatch(memberUpdatedLatch));
            CloseableUtils.closeQuietly(groupMember2);
            timing.sleepABit();
            Assert.assertTrue(timing.awaitLatch(memberLeftLatch));
        }
        finally
        {
            CloseableUtils.closeQuietly(groupMember1);
            CloseableUtils.closeQuietly(client);
        }
    }
}
