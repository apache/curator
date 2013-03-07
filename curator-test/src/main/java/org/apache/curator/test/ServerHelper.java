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

package org.apache.curator.test;

import org.apache.zookeeper.server.ZooKeeperServer;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;

class ServerHelper
{
    private static class ServerCnxnFactoryMethods
    {
        private final Constructor constructor;
        private final Method configureMethod;
        private final Method startupMethod;
        private final Method shutdownMethod;

        private ServerCnxnFactoryMethods(Constructor constructor, Method configureMethod, Method startupMethod, Method shutdownMethod)
        {
            this.constructor = constructor;
            this.configureMethod = configureMethod;
            this.startupMethod = startupMethod;
            this.shutdownMethod = shutdownMethod;
        }
    }

    private static class NioServerCnxnMethods
    {
        private final Constructor constructor;
        private final Method startupMethod;
        private final Method shutdownMethod;

        private NioServerCnxnMethods(Constructor constructor, Method startupMethod, Method shutdownMethod)
        {
            this.constructor = constructor;
            this.startupMethod = startupMethod;
            this.shutdownMethod = shutdownMethod;
        }
    }

    private static final ServerCnxnFactoryMethods       serverCnxnFactoryMethods;
    private static final NioServerCnxnMethods nioServerCnxn;

    static
    {
        Class       serverCnxnFactoryClass = null;
        Class       nioServerCnxnFactoryClass = null;
        try
        {
            serverCnxnFactoryClass = Class.forName("org.apache.zookeeper.server.NIOServerCnxnFactory");
        }
        catch ( ClassNotFoundException ignore )
        {
            // ignore
        }

        try
        {
            nioServerCnxnFactoryClass = Class.forName("org.apache.zookeeper.server.NIOServerCnxn$Factory");
        }
        catch ( ClassNotFoundException ignore )
        {
            // ignore
        }

        ServerCnxnFactoryMethods        localServerCnxnFactoryMethods = null;
        NioServerCnxnMethods localNioServerCnxn = null;
        try
        {
            if ( serverCnxnFactoryClass != null )
            {
                localServerCnxnFactoryMethods = new ServerCnxnFactoryMethods
                (
                    serverCnxnFactoryClass.getConstructor(),
                    serverCnxnFactoryClass.getDeclaredMethod("configure", InetSocketAddress.class, Integer.TYPE),
                    serverCnxnFactoryClass.getDeclaredMethod("startup", ZooKeeperServer.class),
                    serverCnxnFactoryClass.getDeclaredMethod("shutdown")
                );
            }
            else if ( nioServerCnxnFactoryClass != null )
            {
                localNioServerCnxn = new NioServerCnxnMethods
                (
                    nioServerCnxnFactoryClass.getConstructor(InetSocketAddress.class),
                    nioServerCnxnFactoryClass.getDeclaredMethod("startup", ZooKeeperServer.class),
                    nioServerCnxnFactoryClass.getDeclaredMethod("shutdown")
                );
            }
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw new Error(e);
        }

        serverCnxnFactoryMethods = localServerCnxnFactoryMethods;
        nioServerCnxn = localNioServerCnxn;
    }

    static Object       makeFactory(ZooKeeperServer server, int port) throws Exception
    {
        Object      factory;
        if ( nioServerCnxn != null )
        {
            factory = nioServerCnxn.constructor.newInstance(new InetSocketAddress(port));
            if ( server != null )
            {
                nioServerCnxn.startupMethod.invoke(factory, server);
            }
        }
        else
        {
            factory = serverCnxnFactoryMethods.constructor.newInstance();
            serverCnxnFactoryMethods.configureMethod.invoke(factory, new InetSocketAddress(port), 0);
            if ( server != null )
            {
                serverCnxnFactoryMethods.startupMethod.invoke(factory, server);
            }
        }
        return factory;
    }
    
    static void         shutdownFactory(Object factory)
    {
        try
        {
            if ( nioServerCnxn != null )
            {
                nioServerCnxn.shutdownMethod.invoke(factory);
            }
            else
            {
                serverCnxnFactoryMethods.shutdownMethod.invoke(factory);
            }
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw new Error(e);
        }
    }

    private ServerHelper()
    {
    }
}
