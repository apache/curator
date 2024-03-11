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

package org.apache.curator.zk36;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.curator.utils.Compatibility;
import org.apache.curator.utils.ZookeeperCompatibility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class TestIs36 extends CuratorTestBase {
    @Test
    @Tag(zk36Group)
    public void testIsZk36() {
        assertTrue(Compatibility.hasGetReachableOrOneMethod());
        assertTrue(Compatibility.hasAddrField());
        assertTrue(Compatibility.hasPersistentWatchers());
        assertTrue(ZookeeperCompatibility.LATEST.hasPersistentWatchers());
        assertFalse(ZookeeperCompatibility.builder()
            .build()
            .hasPersistentWatchers());
        assertFalse(ZookeeperCompatibility.builder()
                .hasPersistentWatchers(false)
                .build()
                .hasPersistentWatchers());
        try {
            Class.forName("org.apache.zookeeper.proto.WhoAmIResponse");
            fail("WhoAmIResponse is introduced after ZooKeeper 3.7");
        } catch (ClassNotFoundException ignore) {
        }
    }

    @Override
    protected void createServer() {
        // NOP
    }
}
