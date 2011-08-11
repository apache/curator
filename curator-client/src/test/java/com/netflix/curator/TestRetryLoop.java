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
package com.netflix.curator;

import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.utils.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.File;

public class TestRetryLoop extends BaseClassForTests
{
    @Test
    public void     testRetryLoopWithFailure() throws Exception
    {
        int                 serverPort = server.getPort();
        File                tempDirectory = server.getTempDirectory();

        CuratorZookeeperClient client = new CuratorZookeeperClient(server.getConnectString(), 3000, 3000, null, new RetryOneTime(1));
        client.start();
        try
        {
            int         loopCount = 0;
            RetryLoop   retryLoop = client.newRetryLoop();
            outer: while ( retryLoop.shouldContinue()  )
            {
                ++loopCount;
                switch ( loopCount )
                {
                    case 1:
                    {
                        server.stop();
                        break;
                    }

                    case 2:
                    {
                        server = new TestingServer(serverPort, tempDirectory);
                        break;
                    }

                    case 3:
                    case 4:
                    {
                        // ignore
                        break;
                    }

                    default:
                    {
                        Assert.fail();
                        break outer;
                    }
                }

                try
                {
                    client.getZooKeeper().create("/test", new byte[]{1,2,3}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    retryLoop.markComplete();
                }
                catch ( Exception e )
                {
                    retryLoop.takeException(e);
                }
            }

            Assert.assertTrue(loopCount >= 2);
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testRetryLoop() throws Exception
    {
        CuratorZookeeperClient client = new CuratorZookeeperClient(server.getConnectString(), 10000, 10000, null, new RetryOneTime(1));
        client.start();
        try
        {
            int         loopCount = 0;
            RetryLoop   retryLoop = client.newRetryLoop();
            while ( retryLoop.shouldContinue()  )
            {
                if ( ++loopCount > 2 )
                {
                    Assert.fail();
                    break;
                }

                try
                {
                    client.getZooKeeper().create("/test", new byte[]{1,2,3}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    retryLoop.markComplete();
                }
                catch ( Exception e )
                {
                    retryLoop.takeException(e);
                }
            }

            Assert.assertTrue(loopCount > 0);
        }
        finally
        {
            client.close();
        }
    }
}
