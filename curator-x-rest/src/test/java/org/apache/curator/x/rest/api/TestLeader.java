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

package org.apache.curator.x.rest.api;

import com.google.common.collect.Lists;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.rest.support.BaseClassForTests;
import org.apache.curator.x.rest.support.LeaderLatchBridge;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Collection;
import java.util.List;

public class TestLeader extends BaseClassForTests
{
    private static final int MAX_LOOPS = 5;

    @Test
    public void testBasic() throws Exception
    {
        final String PATH_NAME = "/leader";
        final int PARTICIPANT_QTY = 10;

        List<LeaderLatchBridge> latches = Lists.newArrayList();

        Timing timing = new Timing();
        try
        {
            for ( int i = 0; i < PARTICIPANT_QTY; ++i )
            {
                LeaderLatchBridge latch = new LeaderLatchBridge(restClient, sessionManager, uriMaker, PATH_NAME);
                latch.start();
                latches.add(latch);
            }

            while ( latches.size() > 0 )
            {
                List<LeaderLatchBridge> leaders = waitForALeader(latches, timing);
                Assert.assertEquals(leaders.size(), 1); // there can only be one leader
                LeaderLatchBridge theLeader = leaders.get(0);
                Assert.assertEquals(latches.indexOf(theLeader), 0); // assert ordering - leadership should advance in start order
                theLeader.close();
                latches.remove(theLeader);
            }
        }
        finally
        {
            for ( LeaderLatchBridge latch : latches )
            {
                CloseableUtils.closeQuietly(latch);
            }
        }
    }

    private List<LeaderLatchBridge> waitForALeader(List<LeaderLatchBridge> latches, Timing timing) throws InterruptedException
    {
        for ( int i = 0; i < MAX_LOOPS; ++i )
        {
            List<LeaderLatchBridge> leaders = getLeaders(latches);
            if ( leaders.size() != 0 )
            {
                return leaders;
            }
            timing.sleepABit();
        }
        return Lists.newArrayList();
    }

    private List<LeaderLatchBridge> getLeaders(Collection<LeaderLatchBridge> latches)
    {
        List<LeaderLatchBridge> leaders = Lists.newArrayList();
        for ( LeaderLatchBridge latch : latches )
        {
            if ( latch.hasLeadership() )
            {
                leaders.add(latch);
            }
        }
        return leaders;
    }
}
