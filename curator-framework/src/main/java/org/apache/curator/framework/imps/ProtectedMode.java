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

package org.apache.curator.framework.imps;

import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

/**
 * manage the protected mode state for {@link org.apache.curator.framework.imps.CreateBuilderImpl}
 */
class ProtectedMode
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private volatile boolean doProtected = false;
    private volatile String protectedId = null;
    private volatile long sessionId = 0;

    /**
     * Enable protected mode
     */
    void setProtectedMode()
    {
        doProtected = true;
        resetProtectedId();
    }

    /**
     * Update the protected mode ID
     */
    void resetProtectedId()
    {
        protectedId = UUID.randomUUID().toString();
    }

    /**
     * @return true if protected mode has been enabled
     */
    boolean doProtected()
    {
        return doProtected;
    }

    /**
     * @return the protected mode ID if protected mode is enabled
     */
    String protectedId()
    {
        return protectedId;
    }

    /**
     * Record the current session ID if needed
     *
     * @param client current client
     * @param createMode create mode in use
     * @throws Exception errors
     */
    void checkSetSessionId(CuratorFrameworkImpl client, CreateMode createMode) throws Exception
    {
        if ( doProtected && (sessionId == 0) && createMode.isEphemeral() )
        {
            sessionId = client.getZooKeeper().getSessionId();
        }
    }

    /**
     * Validate the found protected-mode node based on the set session ID, etc.
     *
     * @param client current client
     * @param createMode create mode in use
     * @param foundNode the found node
     * @return either the found node or null - client should always use the returned value
     * @throws Exception errors
     */
    String validateFoundNode(CuratorFrameworkImpl client, CreateMode createMode, String foundNode) throws Exception
    {
        if ( doProtected && createMode.isEphemeral() )
        {
            long clientSessionId = client.getZooKeeper().getSessionId();
            if ( this.sessionId != clientSessionId )
            {
                log.info("Session has changed during protected mode with ephemeral. old: {} new: {}", this.sessionId, clientSessionId);
                if ( foundNode != null )
                {
                    log.info("Deleted old session's found node: {}", foundNode);
                    client.getFailedDeleteManager().executeGuaranteedOperationInBackground(foundNode);
                    foundNode = null;
                }
                this.sessionId = clientSessionId;
            }
        }
        return foundNode;
    }
}
