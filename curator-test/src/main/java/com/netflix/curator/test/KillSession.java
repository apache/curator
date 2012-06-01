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

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *     Utility to simulate a ZK session dying. See: <a href="http://wiki.apache.org/hadoop/ZooKeeper/FAQ#A4">ZooKeeper FAQ</a>
 * </p>
 *
 * <blockquote>
 *     In the case of testing we want to cause a problem, so to explicitly expire a session an
 *     application connects to ZooKeeper, saves the session id and password, creates another
 *     ZooKeeper handle with that id and password, and then closes the new handle. Since both
 *     handles reference the same session, the close on second handle will invalidate the session
 *     causing a SESSION_EXPIRED on the first handle.
 * </blockquote>
 */
public class KillSession
{
    /**
     * Kill the given ZK session
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
     * Kill the given ZK session
     *
     * @param client the client to kill
     * @param connectString server connection string
     * @param maxMs max time ms to wait for kill
     * @throws Exception errors
     */
    public static void     kill(ZooKeeper client, String connectString, int maxMs) throws Exception
    {
        long                    startTicks = System.currentTimeMillis();

        final CountDownLatch    sessionLostLatch = new CountDownLatch(1);
        Watcher                 sessionLostWatch = new Watcher()
        {
            @Override
            public void process(WatchedEvent event)
            {
                sessionLostLatch.countDown();
            }
        };
        client.exists("/___CURATOR_KILL_SESSION___", sessionLostWatch);

        final CountDownLatch    connectionLatch = new CountDownLatch(1);
        Watcher                 connectionWatcher = new Watcher()
        {
            @Override
            public void process(WatchedEvent event)
            {
                if ( event.getState() == Event.KeeperState.SyncConnected )
                {
                    connectionLatch.countDown();
                }
            }
        };
        ZooKeeper zk = new ZooKeeper(connectString, maxMs, connectionWatcher, client.getSessionId(), client.getSessionPasswd());
        try
        {
            if ( !connectionLatch.await(maxMs, TimeUnit.MILLISECONDS) )
            {
                throw new Exception("KillSession could not establish duplicate session");
            }
            try
            {
                zk.close();
            }
            finally
            {
                zk = null;
            }

            long        elapsed = System.currentTimeMillis() - startTicks;
            long        thisWaitMs = Math.max(1, maxMs - elapsed);
            if ( !sessionLostLatch.await(thisWaitMs, TimeUnit.MILLISECONDS) )
            {
                throw new Exception("KillSession timed out waiting for session to expire");
            }
        }
        finally
        {
            if ( zk != null )
            {
                zk.close();
            }
        }
    }
}
