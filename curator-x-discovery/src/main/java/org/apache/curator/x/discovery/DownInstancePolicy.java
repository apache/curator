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

package org.apache.curator.x.discovery;

import java.util.concurrent.TimeUnit;

/**
 * Abstraction for values that determine when an instance is down
 */
public class DownInstancePolicy {
    private final long timeoutMs;
    private final int errorThreshold;

    private static final long DEFAULT_TIMEOUT_MS = 30000;
    private static final int DEFAULT_THRESHOLD = 2;

    /**
     * Policy with default values
     */
    public DownInstancePolicy() {
        this(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS, DEFAULT_THRESHOLD);
    }

    /**
     * @param timeout window of time for down instances
     * @param unit time unit
     * @param errorThreshold number of errors within time window that denotes a down instance
     */
    public DownInstancePolicy(long timeout, TimeUnit unit, int errorThreshold) {
        this.timeoutMs = unit.toMillis(timeout);
        this.errorThreshold = errorThreshold;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public int getErrorThreshold() {
        return errorThreshold;
    }
}
