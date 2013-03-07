/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.curator.framework.recipes.leader;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionStateListener;

/**
 * Notification for leadership
 *
 * @see LeaderSelector
 */
public interface LeaderSelectorListener extends ConnectionStateListener
{
    /**
     * Called when your instance has been granted leadership. This method
     * should not return until you wish to release leadership
     *
     * @param client the client
     * @throws Exception any errors
     */
    public void         takeLeadership(CuratorFramework client) throws Exception;
}
