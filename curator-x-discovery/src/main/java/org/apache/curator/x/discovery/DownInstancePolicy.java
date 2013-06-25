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

package org.apache.curator.x.discovery;

import java.util.concurrent.TimeUnit;

public class DownInstancePolicy
{
    private final long timeoutMs;
    private final int threshold;

    private static final long DEFAULT_TIMEOUT_MS = 30000;
    private static final int DEFAULT_THRESHOLD = 2;

    public DownInstancePolicy()
    {
        this(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS, DEFAULT_THRESHOLD);
    }

    public DownInstancePolicy(long timeout, TimeUnit unit, int threshold)
    {
        this.timeoutMs = unit.toMillis(timeout);
        this.threshold = threshold;
    }

    public long getTimeoutMs()
    {
        return timeoutMs;
    }

    public int getThreshold()
    {
        return threshold;
    }
}
