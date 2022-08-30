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

package org.apache.curator.utils;

import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;

/**
 * Utils to help with ZK version compatibility
 */
public class Compatibility
{
    private static final Logger log = LoggerFactory.getLogger(Compatibility.class);

    private static final Method getReachableOrOneMethod;
    private static final Field addrField;
    private static final boolean hasPersistentWatchers;

    static
    {
        Method localGetReachableOrOneMethod;
        try
        {
            Class<?> multipleAddressesClass = Class.forName("org.apache.zookeeper.server.quorum.MultipleAddresses");
            localGetReachableOrOneMethod = multipleAddressesClass.getMethod("getReachableOrOne");
            log.info("Using org.apache.zookeeper.server.quorum.MultipleAddresses");
        }
        catch ( ReflectiveOperationException ignore )
        {
            localGetReachableOrOneMethod = null;
        }
        getReachableOrOneMethod = localGetReachableOrOneMethod;

        Field localAddrField;
        try
        {
            localAddrField = QuorumPeer.QuorumServer.class.getField("addr");
        }
        catch ( NoSuchFieldException e )
        {
            localAddrField = null;
            log.error("Could not get addr field! Reconfiguration fail!");
        }
        addrField = localAddrField;

        boolean localHasPersistentWatchers;
        try
        {
            Class.forName("org.apache.zookeeper.AddWatchMode");
            localHasPersistentWatchers = true;
        }
        catch ( ClassNotFoundException e )
        {
            localHasPersistentWatchers = false;
            log.info("Persistent Watchers are not available in the version of the ZooKeeper library being used");
        }
        hasPersistentWatchers = localHasPersistentWatchers;
    }

    public static boolean hasGetReachableOrOneMethod()
    {
        return (getReachableOrOneMethod != null);
    }

    public static boolean hasAddrField()
    {
        return (addrField != null);
    }

    public static String getHostAddress(QuorumPeer.QuorumServer server)
    {
        InetSocketAddress address = null;
        if ( getReachableOrOneMethod != null )
        {
            try
            {
                address = (InetSocketAddress)getReachableOrOneMethod.invoke(server.addr);
            }
            catch ( Exception e )
            {
                log.error("Could not call getReachableOrOneMethod.invoke({})", server.addr, e);
            }
        }
        else if (addrField != null)
        {
            try
            {
                address = (InetSocketAddress)addrField.get(server);
            }
            catch ( Exception e )
            {
                log.error("Could not call addrField.get({})", server, e);
            }
        }
        return (address != null && address.getAddress() != null) ? address.getAddress().getHostAddress() : "unknown";
    }

    public static boolean hasPersistentWatchers()
    {
        return hasPersistentWatchers;
    }
}
