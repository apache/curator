/*
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

import org.apache.zookeeper.ZooKeeper;

/**
 * <p>
 *     Utility to simulate a ZK session dying.
 * </p>
 */
public class KillSession
{
    /**
     * Kill the given ZK session
     *
     * @param client the client to kill
     * @since 3.0.0
     */
    public static void     kill(ZooKeeper client)
    {
        client.getTestable().injectSessionExpiration();
    }

    /**
     * Kill the given ZK session
     *
     * @param client the client to kill
     * @param connectString server connection string
     * @throws Exception errors
     * @deprecated use {@link #kill(ZooKeeper)} instead
     */
    public static void     kill(ZooKeeper client, String connectString) throws Exception
    {
        kill(client);
    }

    /**
     * Kill the given ZK session
     *
     * @param client the client to kill
     * @param connectString server connection string
     * @param maxMs max time ms to wait for kill
     * @throws Exception errors
     * @deprecated use {@link #kill(ZooKeeper)} instead
     */
    public static void     kill(ZooKeeper client, String connectString, int maxMs) throws Exception
    {
        kill(client);
    }
}
