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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.WatchersDebug;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.ZooKeeper;

public class TestCleanState
{
    public static void closeAndTestClean(CuratorFramework client)
    {
        if ( client == null )
        {
            return;
        }

        try
        {
            CuratorFrameworkImpl internalClient = (CuratorFrameworkImpl)client;
            EnsembleTracker ensembleTracker = internalClient.getEnsembleTracker();
            if ( ensembleTracker != null )
            {
                ensembleTracker.close();
            }
            ZooKeeper zooKeeper = internalClient.getZooKeeper();
            if ( zooKeeper != null )
            {
                if ( WatchersDebug.getChildWatches(zooKeeper).size() != 0 )
                {
                    throw new AssertionError("One or more child watchers are still registered: " + WatchersDebug.getChildWatches(zooKeeper));
                }
                if ( WatchersDebug.getExistWatches(zooKeeper).size() != 0 )
                {
                    throw new AssertionError("One or more exists watchers are still registered: " + WatchersDebug.getExistWatches(zooKeeper));
                }
                if ( WatchersDebug.getDataWatches(zooKeeper).size() != 0 )
                {
                    throw new AssertionError("One or more data watchers are still registered: " + WatchersDebug.getDataWatches(zooKeeper));
                }
            }
        }
        catch ( IllegalStateException ignore )
        {
            // client already closed
        }
        catch ( Exception e )
        {
            e.printStackTrace();    // not sure what to do here
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    private TestCleanState()
    {
    }
}
