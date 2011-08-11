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
package com.netflix.curator.framework.recipes.leader;

import com.netflix.curator.framework.CuratorFramework;

/**
 * Notification for leadership
 *
 * @see LeaderSelector
 */
public interface LeaderSelectorListener
{
    /**
     * Called when your instance has been granted leadership. This method
     * should not return until you wish to release leadership
     *
     * @param client the client
     * @throws Exception any errors
     */
    public void         takeLeadership(CuratorFramework client) throws Exception;

    /**
     * Called when an exception that can't be processed internally is caught
     *
     * @param client the client
     * @param exception the exception
     */
    public void         handleException(CuratorFramework client, Exception exception);

    /**
     * Will get called if client connection unexpectedly closes. *IMPORTANT* this
     * method can get called after the {@link #takeLeadership(CuratorFramework)} method has been called.
     *
     * @param client the client
     */
    public void         notifyClientClosing(CuratorFramework client);
}
