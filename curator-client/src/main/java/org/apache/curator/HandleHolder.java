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

import org.apache.curator.ensemble.EnsembleProvider;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

class HandleHolder
{
    private final ZookeeperFactory zookeeperFactory;
    private final Watcher watcher;
    private final EnsembleProvider ensembleProvider;
    private final int sessionTimeout;
    private final boolean canBeReadOnly;

    private volatile Helper helper;

    private interface Helper
    {
        ZooKeeper getZooKeeper() throws Exception;
        
        String getConnectionString();

        int getNegotiatedSessionTimeoutMs();
    }

    HandleHolder(ZookeeperFactory zookeeperFactory, Watcher watcher, EnsembleProvider ensembleProvider, int sessionTimeout, boolean canBeReadOnly)
    {
        this.zookeeperFactory = zookeeperFactory;
        this.watcher = watcher;
        this.ensembleProvider = ensembleProvider;
        this.sessionTimeout = sessionTimeout;
        this.canBeReadOnly = canBeReadOnly;
    }

    ZooKeeper getZooKeeper() throws Exception
    {
        return (helper != null) ? helper.getZooKeeper() : null;
    }

    int getNegotiatedSessionTimeoutMs()
    {
        return (helper != null) ? helper.getNegotiatedSessionTimeoutMs() : 0;
    }

    String  getConnectionString()
    {
        return (helper != null) ? helper.getConnectionString() : null;
    }

    String getNewConnectionString()
    {
        String helperConnectionString = (helper != null) ? helper.getConnectionString() : null;
        return ((helperConnectionString != null) && !ensembleProvider.getConnectionString().equals(helperConnectionString)) ? helperConnectionString : null;
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
                        zooKeeperHandle = zookeeperFactory.newZooKeeper(connectionString, sessionTimeout, watcher, canBeReadOnly);
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

                        @Override
                        public int getNegotiatedSessionTimeoutMs()
                        {
                            return (zooKeeperHandle != null) ? zooKeeperHandle.getSessionTimeout() : 0;
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

            @Override
            public int getNegotiatedSessionTimeoutMs()
            {
                return (zooKeeperHandle != null) ? zooKeeperHandle.getSessionTimeout() : 0;
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
                Watcher dummyWatcher = new Watcher()
                {
                    @Override
                    public void process(WatchedEvent event)
                    {
                    }
                };
                zooKeeper.register(dummyWatcher);   // clear the default watcher so that no new events get processed by mistake
                zooKeeper.close();
            }
        }
        catch ( InterruptedException dummy )
        {
            Thread.currentThread().interrupt();
        }
    }
}
