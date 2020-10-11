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
package org.apache.curator.framework.imps;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.EnsureContainers;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.utils.CloseableUtils;
import org.junit.jupiter.api.Test;

public class TestEnsureContainers extends BaseClassForTests
{
    @Test
    public void testBasic() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            EnsureContainers ensureContainers = new EnsureContainers(client, "/one/two/three");
            ensureContainers.ensure();

            assertNotNull(client.checkExists().forPath("/one/two/three"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testSingleExecution() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            EnsureContainers ensureContainers = new EnsureContainers(client, "/one/two/three");
            ensureContainers.ensure();

            assertNotNull(client.checkExists().forPath("/one/two/three"));

            client.delete().forPath("/one/two/three");
            ensureContainers.ensure();
            assertNull(client.checkExists().forPath("/one/two/three"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }
}
