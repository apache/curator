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

package org.apache.curator.utils;

import java.util.concurrent.TimeUnit;
import org.apache.curator.drivers.TracerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default tracer driver
 */
public class DefaultTracerDriver implements TracerDriver {
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public void addTrace(String name, long time, TimeUnit unit) {
        if (log.isTraceEnabled()) {
            log.trace("Trace: " + name + " - " + TimeUnit.MILLISECONDS.convert(time, unit) + " ms");
        }
    }

    @Override
    public void addCount(String name, int increment) {
        if (log.isTraceEnabled()) {
            log.trace("Counter " + name + ": " + increment);
        }
    }
}
