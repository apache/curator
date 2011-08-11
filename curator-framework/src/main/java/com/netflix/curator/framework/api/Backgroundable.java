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

import java.util.concurrent.Executor;

public interface Backgroundable<T>
{
    /**
     * Perform the action in the background
     *
     * @return this
     */
    public T inBackground();

    /**
     * Perform the action in the background
     *
     * @param context context object - will be available from the event sent to the listener
     * @return this
     */
    public T inBackground(Object context);

    /**
     * Perform the action in the background
     *
     * @param callback a functor that will get called when the operation has completed
     * @return this
     */
    public T inBackground(BackgroundCallback callback);

    /**
     * Perform the action in the background
     *
     * @param callback a functor that will get called when the operation has completed
     * @param executor executor to use for the background call
     * @return this
     */
    public T inBackground(BackgroundCallback callback, Executor executor);
}
