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
package org.apache.curator.drivers;

import java.util.concurrent.TimeUnit;

/**
 * Mechanism for timing methods and recording counters
 */
public interface TracerDriver
{
    /**
     * Record the given trace event
     *
     * @param name of the event
     * @param time time event took
     * @param unit time unit
     */
    public void     addTrace(String name, long time, TimeUnit unit);

    /**
     * Add to a named counter
     *
     * @param name name of the counter
     * @param increment amount to increment
     */
    public void     addCount(String name, int increment);
}
