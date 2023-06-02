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

package org.apache.curator.framework.listen;

import java.util.concurrent.Executor;

/**
 * Abstracts a listenable object
 */
public interface Listenable<T> {
    /**
     * Add the given listener. The listener will be executed in the containing
     * instance's thread.
     *
     * @param listener listener to add
     */
    public void addListener(T listener);

    /**
     * Add the given listener. The listener will be executed using the given
     * executor
     *
     * @param listener listener to add
     * @param executor executor to run listener in
     */
    public void addListener(T listener, Executor executor);

    /**
     * Remove the given listener
     *
     * @param listener listener to remove
     */
    public void removeListener(T listener);
}
