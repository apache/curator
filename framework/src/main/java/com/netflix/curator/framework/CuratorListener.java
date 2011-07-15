/*
 Copyright 2011 Netflix, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 */
package com.netflix.curator.framework;

/**
 * Receives notifications about errors and background events
 */
public interface CuratorListener
{
    /**
     * Called when a background task has completed or a watch has triggered
     *
     * @param client client
     * @param event the event
     * @throws Exception any errors
     */
    public void         eventReceived(CuratorFramework client, CuratorEvent event) throws Exception;

    /**
     * Called when all retries have failed. The connection will have been closed.
     *
     * @param client client (now closed)
     * @param resultCode the result code that caused the issue or 0
     * @param e the exception that caused the issue or null
     */
    public void         clientClosedDueToError(CuratorFramework client, int resultCode, Throwable e);
}
