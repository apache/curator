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

import org.apache.zookeeper.ClientCnxn;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

// reflective version of zooKeeper.getTestable().injectSessionExpiration();
@SuppressWarnings("JavaReflectionMemberAccess")
public class InjectSessionExpiration
{
    private static final Field cnxnField;
    private static final Field eventThreadField;
    private static final Method queueEventMethod;
    static
    {
        Field localCnxnField;
        Field localEventThreadField;
        Method localQueueEventMethod;
        try
        {
            Class<?> eventThreadClass = Class.forName("org.apache.zookeeper.ClientCnxn$EventThread");

            localCnxnField = ZooKeeper.class.getDeclaredField("cnxn");
            localCnxnField.setAccessible(true);
            localEventThreadField = ClientCnxn.class.getDeclaredField("eventThread");
            localEventThreadField.setAccessible(true);
            localQueueEventMethod = eventThreadClass.getDeclaredMethod("queueEvent", WatchedEvent.class);
            localQueueEventMethod.setAccessible(true);
        }
        catch ( ReflectiveOperationException e )
        {
            throw new RuntimeException("Could not access internal ZooKeeper fields", e);
        }
        cnxnField = localCnxnField;
        eventThreadField = localEventThreadField;
        queueEventMethod = localQueueEventMethod;
    }

    public static void injectSessionExpiration(ZooKeeper zooKeeper)
    {
        try
        {
            WatchedEvent event = new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.Expired, null);

            ClientCnxn clientCnxn = (ClientCnxn)cnxnField.get(zooKeeper);
            Object eventThread = eventThreadField.get(clientCnxn);
            queueEventMethod.invoke(eventThread, event);

            // we used to set the state field to CLOSED here and a few other things but this resulted in CURATOR-498
        }
        catch ( ReflectiveOperationException e )
        {
            throw new RuntimeException("Could not inject session expiration using reflection", e);
        }
    }
}
