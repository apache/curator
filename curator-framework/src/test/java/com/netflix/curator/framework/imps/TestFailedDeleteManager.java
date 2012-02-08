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

package com.netflix.curator.framework.imps;

import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.test.TestingServer;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.File;

public class TestFailedDeleteManager extends BaseClassForTests
{
    @Test
    public void     testBasic() throws Exception
    {
        final String PATH = "/one/two/three";

        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        builder.connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1)).connectionTimeoutMs(5000).sessionTimeoutMs(100000);
        CuratorFrameworkImpl            client = new CuratorFrameworkImpl(builder);
        client.start();
        try
        {
            client.create().creatingParentsIfNeeded().forPath(PATH);
            Assert.assertNotNull(client.checkExists().forPath(PATH));

            File    serverDir = server.getTempDirectory();
            int     serverPort = server.getPort();

            server.close(); // cause the next delete to fail
            server = null;
            try
            {
                client.delete().forPath(PATH);
                Assert.fail();
            }
            catch ( Exception expected )
            {
                // expected
            }
            
            server = new TestingServer(serverPort, serverDir);
            Assert.assertNotNull(client.checkExists().forPath(PATH));

            server.close(); // cause the next delete to fail
            server = null;
            try
            {
                client.delete().guaranteed().forPath(PATH);
                Assert.fail();
            }
            catch ( Exception expected )
            {
                // expected
            }

            server = new TestingServer(serverPort, serverDir);

            final int       TRIES = 5;
            for ( int i = 0; i < TRIES; ++i )
            {
                if ( client.checkExists().forPath(PATH) != null )
                {
                    Thread.sleep(500 * i);
                }
            }
            Assert.assertNull(client.checkExists().forPath(PATH));
        }
        finally
        {
            Closeables.closeQuietly(client);
        }
    }
}
