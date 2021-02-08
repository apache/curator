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
package org.apache.curator.test;

import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerCnxnFactory;
import java.lang.reflect.Method;

@SuppressWarnings({"unchecked", "rawtypes"})
public class Compatibility
{
    private static final Method closeAllWithReasonMethod;
    private static final Method closeAllMethod;
    private static final Method closeWithReasonMethod;
    private static final Method closeMethod;
    private static final Object disconnectReasonObj;

    static
    {
        Object localDisconnectReasonObj;
        Method localCloseAllWithReasonMethod;
        Method localCloseAllMethod;
        Method localCloseWithReasonMethod;
        Method localCloseMethod;
        try
        {
            Class disconnectReasonClass = Class.forName("org.apache.zookeeper.server.ServerCnxn$DisconnectReason");
            localDisconnectReasonObj = Enum.valueOf(disconnectReasonClass, "UNKNOWN");
            localCloseAllWithReasonMethod = ServerCnxnFactory.class.getDeclaredMethod("closeAll", disconnectReasonClass);
            localCloseWithReasonMethod = ServerCnxn.class.getDeclaredMethod("close", disconnectReasonClass);
            localCloseAllMethod = null;
            localCloseMethod = null;

            localCloseAllWithReasonMethod.setAccessible(true);
            localCloseWithReasonMethod.setAccessible(true);
        }
        catch ( Throwable e )
        {
            localDisconnectReasonObj = null;
            localCloseAllWithReasonMethod = null;
            localCloseWithReasonMethod = null;
            try
            {
                localCloseAllMethod = ServerCnxnFactory.class.getDeclaredMethod("closeAll");
                localCloseMethod = ServerCnxn.class.getDeclaredMethod("close");

                localCloseAllMethod.setAccessible(true);
                localCloseMethod.setAccessible(true);
            }
            catch ( Throwable ex )
            {
                throw new IllegalStateException("Could not reflectively find ServerCnxnFactory/ServerCnxn close methods");
            }
        }
        disconnectReasonObj = localDisconnectReasonObj;
        closeAllWithReasonMethod = localCloseAllWithReasonMethod;
        closeAllMethod = localCloseAllMethod;
        closeMethod = localCloseMethod;
        closeWithReasonMethod = localCloseWithReasonMethod;
    }

    public static void serverCnxnFactoryCloseAll(ServerCnxnFactory factory)
    {
        try
        {
            if ( closeAllMethod != null )
            {
                closeAllMethod.invoke(factory);
            }
            else
            {
                closeAllWithReasonMethod.invoke(factory, disconnectReasonObj);
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException("Could not close factory", e);
        }
    }

    public static void serverCnxnClose(ServerCnxn cnxn)
    {
        try
        {
            if ( closeMethod != null )
            {
                closeMethod.invoke(cnxn);
            }
            else
            {
                closeWithReasonMethod.invoke(cnxn, disconnectReasonObj);
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException("Could not close connection", e);
        }
    }
}
