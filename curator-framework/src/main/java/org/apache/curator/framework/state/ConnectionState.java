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
package org.apache.curator.framework.state;

import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

/**
 * Represents state changes in the connection to ZK
 */
public enum ConnectionState
{
    /**
     * Sent for the first successful connection to the server. NOTE: You will only
     * get one of these messages for any CuratorFramework instance.
     */
    CONNECTED
    {
        public boolean isConnected()
        {
            return true;
        }
    },

    /**
     * There has been a loss of connection. Leaders, locks, etc. should suspend
     * until the connection is re-established.
     */
    SUSPENDED
    {
        public boolean isConnected()
        {
            return false;
        }
    },    

    /**
     * A suspended, lost, or read-only connection has been re-established
     */
    RECONNECTED
    {
        public boolean isConnected()
        {
            return true;
        }
    },

    /**
     * <p>
     *     NOTE: the meaning of this state depends on how your CuratorFramework instance
     *     is created.
     * </p>
     *
     * <p>
     *     The default meaning of LOST (and the only meaning prior to Curator 3.0.0) is:
     *     The connection is confirmed to be lost (i.e. the retry policy has given up). Close any locks, leaders, etc. and
     *     attempt to re-create them. NOTE: it is possible to get a {@link #RECONNECTED}
     *     state after this but you should still consider any locks, etc. as dirty/unstable
     * </p>
     *
     * <p>
     *     <strong>Since 3.0.0</strong>, you can alter the meaning of LOST by calling
     *     {@link CuratorFrameworkFactory.Builder#enableSessionExpiredState()}. In this mode,
     *     Curator will set the LOST state only when it believes that the ZooKeeper session
     *     has expired. ZooKeeper connections have a session. When the session expires, clients must take appropriate
     *     action. In Curator, this is complicated by the fact that Curator internally manages the ZooKeeper
     *     connection. In this mode, Curator will set the LOST state when any of the following occurs:
     *     a) ZooKeeper returns a {@link Watcher.Event.KeeperState#Expired} or {@link KeeperException.Code#SESSIONEXPIRED};
     *     b) Curator closes the internally managed ZooKeeper instance; c) The configured session timeout
     *     elapses during a network partition.
     * </p>
     */
    LOST
    {
        public boolean isConnected()
        {
            return false;
        }
    },

    /**
     * The connection has gone into read-only mode. This can only happen if you pass true
     * for {@link CuratorFrameworkFactory.Builder#canBeReadOnly()}. See the ZooKeeper doc
     * regarding read only connections:
     * <a href="http://wiki.apache.org/hadoop/ZooKeeper/GSoCReadOnlyMode">http://wiki.apache.org/hadoop/ZooKeeper/GSoCReadOnlyMode</a>.
     * The connection will remain in read only mode until another state change is sent.
     */
    READ_ONLY
    {
        public boolean isConnected()
        {
            return true;
        }
    }

    ;
    
    /**
     * Check if this state indicates that Curator has a connection to ZooKeeper
     * 
     * @return True if connected, false otherwise
     */
    public abstract boolean isConnected();
}
