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

package org.apache.curator;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.drivers.EventTrace;
import org.apache.curator.drivers.TracerDriver;
import org.apache.curator.utils.DebugUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RetryLoopImpl extends RetryLoop {
    private boolean isDone = false;
    private int retryCount = 0;

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final long startTimeMs = System.currentTimeMillis();
    private final RetryPolicy retryPolicy;
    private final AtomicReference<TracerDriver> tracer;

    private static final RetrySleeper sleeper = (time, unit) -> unit.sleep(time);

    RetryLoopImpl(RetryPolicy retryPolicy, AtomicReference<TracerDriver> tracer) {
        this.retryPolicy = retryPolicy;
        this.tracer = tracer;
    }

    static RetrySleeper getRetrySleeper() {
        return sleeper;
    }

    @Override
    public boolean shouldContinue() {
        return !isDone;
    }

    @Override
    public void markComplete() {
        isDone = true;
    }

    @Override
    public void takeException(Exception exception) throws Exception {
        boolean rethrow = true;
        if (retryPolicy.allowRetry(exception)) {
            if (!Boolean.getBoolean(DebugUtils.PROPERTY_DONT_LOG_CONNECTION_ISSUES)) {
                log.debug("Retry-able exception received", exception);
            }

            if (retryPolicy.allowRetry(retryCount++, System.currentTimeMillis() - startTimeMs, sleeper)) {
                new EventTrace("retries-allowed", tracer.get()).commit();
                if (!Boolean.getBoolean(DebugUtils.PROPERTY_DONT_LOG_CONNECTION_ISSUES)) {
                    log.debug("Retrying operation");
                }
                rethrow = false;
            } else {
                new EventTrace("retries-disallowed", tracer.get()).commit();
                if (!Boolean.getBoolean(DebugUtils.PROPERTY_DONT_LOG_CONNECTION_ISSUES)) {
                    log.debug("Retry policy not allowing retry");
                }
            }
        }

        if (rethrow) {
            throw exception;
        }
    }
}
