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
package org.apache.curator.framework;

import static org.junit.jupiter.api.Assertions.assertThrows;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.Test;

public class TestCompatibility extends CuratorTestBase
{
    @Test
    public void testPersistentWatchesNotAvailable()
    {
        assertThrows(IllegalStateException.class, ()-> {
            try (CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1))) {
                client.start();
                client.watchers().add().forPath("/foo");
            }
        });
    }

    @Test
    public void testPersistentWatchesNotAvailableAsync()
    {
        assertThrows(IllegalStateException.class, ()->{
            try ( CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1)) )
            {
                client.start();

                AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);
                async.addWatch().forPath("/foo");
            }
        });
    }
}
