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

import com.netflix.curator.ensemble.EnsembleProvider;
import com.netflix.curator.utils.ZookeeperFactory;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

class HandleHolder
{
    private final ZookeeperFactory zookeeperFactory;
    private final Watcher watcher;
    private final EnsembleProvider ensembleProvider;
    private final int sessionTimeout;

    private volatile Helper helper;

    private interface Helper
    {
        ZooKeeper getZooKeeper() throws Exception;
        
        String getConnectionString();
    }

    HandleHolder(ZookeeperFactory zookeeperFactory, Watcher watcher, EnsembleProvider ensembleProvider, int sessionTimeout)
    {
        this.zookeeperFactory = zookeeperFactory;
        this.watcher = watcher;
        this.ensembleProvider = ensembleProvider;
        this.sessionTimeout = sessionTimeout;
    }

    ZooKeeper getZooKeeper() throws Exception
    {
        return helper.getZooKeeper();
    }

    String  getConnectionString()
    {
        return (helper != null) ? helper.getConnectionString() : null;
    }

    boolean hasNewConnectionString() 
    {
        String helperConnectionString = (helper != null) ? helper.getConnectionString() : null;
        return (helperConnectionString != null) && !ensembleProvider.getConnectionString().equals(helperConnectionString);
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
            private volatile String connectionString = null;

            @Override
            public ZooKeeper getZooKeeper() throws Exception
            {
                synchronized(this)
                {
                    if ( zooKeeperHandle == null )
                    {
                        connectionString = ensembleProvider.getConnectionString();
                        zooKeeperHandle = zookeeperFactory.newZooKeeper(connectionString, sessionTimeout, watcher);
                    }

                    helper = new Helper()
                    {
                        @Override
                        public ZooKeeper getZooKeeper() throws Exception
                        {
                            return zooKeeperHandle;
                        }

                        @Override
                        public String getConnectionString()
                        {
                            return connectionString;
                        }
                    };

                    return zooKeeperHandle;
                }
            }

            @Override
            public String getConnectionString()
            {
                return connectionString;
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
