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
    private static final Field stateField;
    private static final Field eventThreadField;
    private static final Field sendThreadField;
    private static final Method queueEventMethod;
    private static final Method queueEventOfDeathMethod;
    private static final Method getClientCnxnSocketMethod;
    private static final Method wakeupCnxnMethod;
    static
    {
        Field localCnxnField;
        Field localStateField;
        Field localEventThreadField;
        Field localSendThreadField;
        Method localQueueEventMethod;
        Method localEventOfDeathMethod;
        Method localGetClientCnxnSocketMethod;
        Method localWakeupCnxnMethod;
        try
        {
            Class<?> eventThreadClass = Class.forName("org.apache.zookeeper.ClientCnxn$EventThread");
            Class<?> sendThreadClass = Class.forName("org.apache.zookeeper.ClientCnxn$SendThread");
            Class<?> clientCnxnSocketClass = Class.forName("org.apache.zookeeper.ClientCnxnSocket");

            localCnxnField = ZooKeeper.class.getDeclaredField("cnxn");
            localCnxnField.setAccessible(true);
            localStateField = ClientCnxn.class.getDeclaredField("state");
            localStateField.setAccessible(true);
            localEventThreadField = ClientCnxn.class.getDeclaredField("eventThread");
            localEventThreadField.setAccessible(true);
            localSendThreadField = ClientCnxn.class.getDeclaredField("sendThread");
            localSendThreadField.setAccessible(true);
            localQueueEventMethod = eventThreadClass.getDeclaredMethod("queueEvent", WatchedEvent.class);
            localQueueEventMethod.setAccessible(true);
            localEventOfDeathMethod = eventThreadClass.getDeclaredMethod("queueEventOfDeath");
            localEventOfDeathMethod.setAccessible(true);
            localGetClientCnxnSocketMethod = sendThreadClass.getDeclaredMethod("getClientCnxnSocket");
            localGetClientCnxnSocketMethod.setAccessible(true);
            localWakeupCnxnMethod = clientCnxnSocketClass.getDeclaredMethod("wakeupCnxn");
            localWakeupCnxnMethod.setAccessible(true);
        }
        catch ( ReflectiveOperationException e )
        {
            throw new RuntimeException("Could not access internal ZooKeeper fields", e);
        }
        cnxnField = localCnxnField;
        stateField = localStateField;
        eventThreadField = localEventThreadField;
        sendThreadField = localSendThreadField;
        queueEventMethod = localQueueEventMethod;
        queueEventOfDeathMethod = localEventOfDeathMethod;
        getClientCnxnSocketMethod = localGetClientCnxnSocketMethod;
        wakeupCnxnMethod = localWakeupCnxnMethod;
    }

    public static void injectSessionExpiration(ZooKeeper zooKeeper)
    {
        try
        {
            WatchedEvent event = new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.Expired, null);

            ClientCnxn clientCnxn = (ClientCnxn)cnxnField.get(zooKeeper);
            Object eventThread = eventThreadField.get(clientCnxn);
            queueEventMethod.invoke(eventThread, event);
            queueEventOfDeathMethod.invoke(eventThread);
            stateField.set(clientCnxn, ZooKeeper.States.CLOSED);
            Object sendThread = sendThreadField.get(clientCnxn);
            Object clientCnxnSocket = getClientCnxnSocketMethod.invoke(sendThread);
            wakeupCnxnMethod.invoke(clientCnxnSocket);
        }
        catch ( ReflectiveOperationException e )
        {
            throw new RuntimeException("Could not inject session expiration using reflection", e);
        }
    }
}
