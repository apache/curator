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

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.reflect.Method;

/**
 * Utils to help with ZK 3.4.x compatibility
 */
public class Compatibility
{
    private static final boolean hasZooKeeperAdmin;
    private static final Method queueEventMethod;
    private static final Logger logger = LoggerFactory.getLogger(Compatibility.class);

    static
    {
        boolean localHasZooKeeperAdmin;
        try
        {
            Class.forName("org.apache.zookeeper.admin.ZooKeeperAdmin");
            localHasZooKeeperAdmin = true;
        }
        catch ( ClassNotFoundException e )
        {
            localHasZooKeeperAdmin = false;
            logger.info("Running in ZooKeeper 3.4.x compatibility mode");
        }
        hasZooKeeperAdmin = localHasZooKeeperAdmin;

        Method localQueueEventMethod;
        try
        {
            Class<?> testableClass = Class.forName("org.apache.zookeeper.Testable");
            localQueueEventMethod = testableClass.getMethod("queueEvent", WatchedEvent.class);
        }
        catch ( ReflectiveOperationException ignore )
        {
            localQueueEventMethod = null;
            LoggerFactory.getLogger(Compatibility.class).info("Using emulated InjectSessionExpiration");
        }
        queueEventMethod = localQueueEventMethod;
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
        if ( isZK34() || (queueEventMethod == null) )
        {
            InjectSessionExpiration.injectSessionExpiration(zooKeeper);
        }
        else
        {
            try
            {
                WatchedEvent event = new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.Expired, null);
                queueEventMethod.invoke(zooKeeper.getTestable(), event);
            }
            catch ( Exception e )
            {
                logger.error("Could not call Testable.queueEvent()", e);
            }
        }
    }
}
