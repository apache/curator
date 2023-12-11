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

import com.google.common.base.Throwables;

/**
 * Utility to accumulate multiple potential exceptions into one that
 * is thrown at the end
 */
public class ExceptionAccumulator {
    private volatile Throwable mainEx = null;

    /**
     * If there is an accumulated exception, throw it
     */
    public void propagate() {
        if (mainEx != null) {
            Throwables.propagate(mainEx);
        }
    }

    /**
     * Add an exception into the accumulated exceptions. Note:
     * if the exception is {@link java.lang.InterruptedException}
     * then <code>Thread.currentThread().interrupt()</code> is called.
     *
     * @param e the exception
     */
    public void add(Throwable e) {
        if (e instanceof InterruptedException) {
            if (mainEx != null) {
                e.addSuppressed(mainEx);
            }
            Thread.currentThread().interrupt();
        }

        if (mainEx == null) {
            mainEx = e;
        } else {
            mainEx.addSuppressed(e);
        }
    }
}
