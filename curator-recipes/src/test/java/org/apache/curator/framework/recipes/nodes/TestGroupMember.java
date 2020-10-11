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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.google.common.base.Function;
import com.google.common.collect.Maps;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.curator.utils.CloseableUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

@Tag(CuratorTestBase.zk35TestCompatibilityGroup)
public class TestGroupMember extends BaseClassForTests
{
    // NOTE - don't need many tests as this class is just a wrapper around two existing recipes

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
            assertTrue(groupMember1.getCurrentMembers().containsKey("1"));
            groupMember1.start();

            groupMember2 = new GroupMember(client, "/member", "2");
            groupMember2.start();

            timing.sleepABit();

            Map<String, byte[]> currentMembers1 = groupMember1.getCurrentMembers();
            Map<String, byte[]> currentMembers2 = groupMember2.getCurrentMembers();
            Map<String, String> convertMembers1 = Maps.transformValues(currentMembers1, new Function<byte[], String>()
            {
                @Override
                public String apply(byte[] input)
                {
                    return new String(input);
                }
            });
            Map<String, String> convertMembers2 = Maps.transformValues(currentMembers1, new Function<byte[], String>()
            {
                @Override
                public String apply(byte[] input)
                {
                    return new String(input);
                }
            });
            assertEquals(convertMembers1.size(), 2);
            assertEquals(convertMembers2.size(), 2);
            assertEquals(convertMembers1, convertMembers2);
            assertTrue(convertMembers1.containsKey("1"));
            assertTrue(convertMembers1.containsKey("2"));

            groupMember2.close();

            timing.sleepABit();

            currentMembers1 = groupMember1.getCurrentMembers();
            assertEquals(currentMembers1.size(), 1);
            assertTrue(currentMembers1.containsKey("1"));
            assertFalse(currentMembers1.containsKey("2"));

            groupMember1.setThisData("something".getBytes());

            timing.sleepABit();
            currentMembers1 = groupMember1.getCurrentMembers();
            assertTrue(currentMembers1.containsKey("1"));
            assertArrayEquals(currentMembers1.get("1"), "something".getBytes());
        }
        finally
        {
            CloseableUtils.closeQuietly(groupMember1);
            CloseableUtils.closeQuietly(groupMember2);
            CloseableUtils.closeQuietly(client);
        }
    }
}
