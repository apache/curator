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

package org.apache.curator.framework.api;

import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;

public interface UnhandledErrorListener
{
    /**
     * Called when an exception is caught in a background thread, handler, etc. Before this
     * listener is called, the error will have been logged and a {@link ConnectionState#LOST}
     * event will have been queued for any {@link ConnectionStateListener}s.
     *
     * @param message Source message
     * @param e exception
     */
    public void     unhandledError(String message, Throwable e);
}
