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

package org.apache.curator.framework.recipes.locks;

import java.util.concurrent.TimeUnit;

public interface InterProcessLock
{
    /**
     * Acquire the mutex - blocking until it's available. Note: the same thread
     * can call acquire re-entrantly. Each call to acquire must be balanced by a call
     * to {@link #release()}
     *
     * @throws Exception ZK errors, interruptions, another thread owns the lock
     */
    public void acquire() throws Exception;

    /**
     * Acquire the mutex - blocks until it's available or the given time expires. Note: the same thread
     * can call acquire re-entrantly. Each call to acquire that returns true must be balanced by a call
     * to {@link #release()}
     *
     * @param time time to wait
     * @param unit time unit
     * @return true if the mutex was acquired, false if not
     * @throws Exception ZK errors, interruptions, another thread owns the lock
     */
    public boolean acquire(long time, TimeUnit unit) throws Exception;

    /**
     * Perform one release of the mutex if the calling thread is the same thread that acquired it. If the
     * thread had made multiple calls to acquire, the mutex will still be held when this method returns.
     *
     * @throws Exception ZK errors, interruptions, current thread does not own the lock
     */
    public void release() throws Exception;

    /**
     * Returns true if the mutex is acquired by a thread in this JVM
     *
     * @return true/false
     */
    boolean isAcquiredInThisProcess();
}
