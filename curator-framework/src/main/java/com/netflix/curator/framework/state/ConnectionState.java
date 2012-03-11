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

package com.netflix.curator.framework.state;

/**
 * Represents state changes in the connection to ZK
 */
public enum ConnectionState
{
    /**
     * Sent for the first successful connection to the server. NOTE: You will only
     * get one of these messages for any CuratorFramework instance.
     */
    CONNECTED,

    /**
     * There has been a loss of connection. Leaders, locks, etc. should suspend
     * until the connection is re-established. If the connection times-out you will
     * receive a {@link #LOST} notice
     */
    SUSPENDED,

    /**
     * A suspended connection has been re-established
     */
    RECONNECTED,

    /**
     * The connection is confirmed to be lost. Close any locks, leaders, etc. and
     * attempt to re-create them. NOTE: it is possible to get a {@link #RECONNECTED}
     * state after this but you should still consider any locks, etc. as dirty/unstable
     */
    LOST
}
