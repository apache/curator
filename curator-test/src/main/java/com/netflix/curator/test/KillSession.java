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

package com.netflix.curator.test;

import org.apache.zookeeper.ZooKeeper;

/**
 * Utility to simulate a ZK session dying
 */
public class KillSession
{
    /**
     * Kill the given ZK sesison
     *
     * @param client the client to kill
     * @param connectString server connection string
     * @throws Exception errors
     */
    public static void     kill(ZooKeeper client, String connectString) throws Exception
    {
        kill(client, connectString, 10000);
    }

    /**
     * Kill the given ZK sesison
     *
     * @param client the client to kill
     * @param connectString server connection string
     * @param maxMs max time ms to wait for kill
     * @throws Exception errors
     */
    public static void     kill(ZooKeeper client, String connectString, int maxMs) throws Exception
    {
        long        startTicks = System.currentTimeMillis();

        ZooKeeper zk = new ZooKeeper(connectString, maxMs, null, client.getSessionId(), client.getSessionPasswd());
        try
        {
            try
            {
                while ( (System.currentTimeMillis() - startTicks) < maxMs )
                {
                    client.exists("/foo", false);
                    Thread.sleep(10);
                }
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
            }
            catch ( Exception expected )
            {
                // this is what we want - the client to drop
            }
        }
        finally
        {
            zk.close(); // this should cause a session error in the main client
        }
    }
}
