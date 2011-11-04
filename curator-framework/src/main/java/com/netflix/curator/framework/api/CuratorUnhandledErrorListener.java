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
package com.netflix.curator.framework.api;

import com.netflix.curator.framework.CuratorFramework;

/**
 * Receives notifications about errors and background events
 */
public interface CuratorUnhandledErrorListener
{
    /**
     * Called when all retries have failed. The connection will still be active but one or more
     * operations may have failed. It is up to the client how to handle this. The safest thing to
     * do is to close the client and create a new client, etc.
     *
     * @param client client
     * @param e the exception that caused the issue
     */
    public void         unhandledError(CuratorFramework client, Throwable e);
}
