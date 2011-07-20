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
package com.netflix.curator;

import com.netflix.curator.drivers.TracerDriver;
import java.util.concurrent.TimeUnit;

/**
 * Utility to time a method or portion of code
 */
public class TimeTrace
{
    private final String name;
    private final TracerDriver driver;
    private final long startTimeNanos = System.nanoTime();

    /**
     * Create and start a timer
     *
     * @param name name of the event
     * @param driver driver
     */
    public TimeTrace(String name, TracerDriver driver)
    {
        this.name = name;
        this.driver = driver;
    }

    /**
     * Record the elapsed time
     */
    public void commit()
    {
        long        elapsed = System.nanoTime() - startTimeNanos;
        driver.addTrace(name, elapsed, TimeUnit.NANOSECONDS);
    }
}
