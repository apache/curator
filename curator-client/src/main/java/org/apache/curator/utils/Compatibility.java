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

import org.apache.zookeeper.ZooKeeper;
import org.slf4j.LoggerFactory;

/**
 * Utils to help with ZK 3.4.x compatibility
 */
public class Compatibility
{
    private static final boolean hasZooKeeperAdmin;
    static
    {
        boolean hasIt;
        try
        {
            Class.forName("org.apache.zookeeper.admin.ZooKeeperAdmin");
            hasIt = true;
        }
        catch ( ClassNotFoundException e )
        {
            hasIt = false;
            LoggerFactory.getLogger(Compatibility.class).info("Running in ZooKeeper 3.4.x compatibility mode");
        }
        hasZooKeeperAdmin = hasIt;
    }

    /**
     * Return true if the classpath ZooKeeper library is 3.4.x
     *
     * @return true/false
     */
    public static boolean isZK34()
    {
        return !hasZooKeeperAdmin;
    }

    /**
     * For ZooKeeper 3.5.x, use the supported <code>zooKeeper.getTestable().injectSessionExpiration()</code>.
     * For ZooKeeper 3.4.x do the equivalent via reflection
     *
     * @param zooKeeper client
     */
    public static void injectSessionExpiration(ZooKeeper zooKeeper)
    {
        if ( isZK34() )
        {
            InjectSessionExpiration.injectSessionExpiration(zooKeeper);
        }
        else
        {
            // LOL - this method was proposed by me (JZ) in 2013 for totally unrelated reasons
            // it got added to ZK 3.5 and now does exactly what we need
            // https://issues.apache.org/jira/browse/ZOOKEEPER-1730
            zooKeeper.getTestable().injectSessionExpiration();
        }
    }
}
