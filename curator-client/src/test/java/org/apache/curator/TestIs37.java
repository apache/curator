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
package org.apache.curator;

import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class TestIs37 extends CuratorTestBase
{
    /**
     * Ensure that ZooKeeper is 3.7 or above.
     *
     * <p>It uses reflection to get {@link ZooKeeper#whoAmI()} which was introduced in 3.7.0.
     *
     * @see <a href="https://issues.apache.org/jira/browse/ZOOKEEPER-3969">ZOOKEEPER-3969</a>
     */
    @Test
    @Tag(zk37Group)
    public void testIsZk37() throws Exception {
        ZooKeeper.class.getMethod("whoAmI");
    }

    @Override
    protected void createServer()
    {
        // NOP
    }
}
