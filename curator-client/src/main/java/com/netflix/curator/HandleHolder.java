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

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

class HandleHolder
{
    private final Watcher watcher;
    private final String connectString;
    private final int sessionTimeout;

    private volatile Helper helper;

    private interface Helper
    {
        ZooKeeper getZooKeeper() throws Exception;
    }

    HandleHolder(final Watcher watcher, final String connectString, final int sessionTimeout)
    {
        this.watcher = watcher;
        this.connectString = connectString;
        this.sessionTimeout = sessionTimeout;
    }

    ZooKeeper getZooKeeper() throws Exception
    {
        return helper.getZooKeeper();
    }

    void closeAndClear() throws Exception
    {
        internalClose();
        helper = null;
    }

    void closeAndReset() throws Exception
    {
        internalClose();

        // first helper is synchronized when getZooKeeper is called. Subsequent calls
        // are not synchronized.
        helper = new Helper()
        {
            private volatile ZooKeeper zooKeeperHandle = null;

            @Override
            public ZooKeeper getZooKeeper() throws Exception
            {
                synchronized(this)
                {
                    if ( zooKeeperHandle == null )
                    {
                        zooKeeperHandle = new ZooKeeper(connectString, sessionTimeout, watcher);
                    }

                    helper = new Helper()
                    {
                        @Override
                        public ZooKeeper getZooKeeper() throws Exception
                        {
                            return zooKeeperHandle;
                        }
                    };

                    return zooKeeperHandle;
                }
            }
        };
    }

    private void internalClose() throws Exception
    {
        try
        {
            ZooKeeper zooKeeper = (helper != null) ? helper.getZooKeeper() : null;
            if ( zooKeeper != null )
            {
                zooKeeper.close();
            }
        }
        catch ( InterruptedException dummy )
        {
            Thread.currentThread().interrupt();
        }
    }
}
