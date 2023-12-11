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

package org.apache.curator.drivers;

import java.util.concurrent.TimeUnit;

/**
 *  Expose more metrics for the operations and events
 */
public abstract class AdvancedTracerDriver implements TracerDriver {
    /**
     * Records the start of a new operation that will later complete successfully or erroneously via a call to
     * {@link #endTrace(OperationTrace)}. The call may optionally return driver specific state which can be
     * accessed in {@link #endTrace(OperationTrace) endTrace} via {@link OperationTrace#getDriverTrace()}. The
     * driver implementation is responsible for checking that any state returned is valid. Additionally, while it is
     * expected that all calls to {@code startTrace} will have a corresponding call to
     * {@link #endTrace(OperationTrace) endTrace}, it is the responsibility of the driver implementation to manage any
     * leaking of non-terminal {@link OperationTrace OperationTraces}.
     *
     * @param trace the metrics of the operation
     * @return The internal trace representation of the driver implementation. Driver dependent, may be {@code null}.
     */
    public abstract Object startTrace(OperationTrace trace);

    /**
     * Signals the completion, successful or otherwise, of the specified {@link OperationTrace}.
     *
     * @param trace the metrics of the operation
     */
    public abstract void endTrace(OperationTrace trace);

    /**
     * Record the given trace event after the completion of the event. This is equivalent to calling
     * {@link #startTrace(OperationTrace) startTrace} followed by {@link #endTrace(OperationTrace) endTrace}.
     *
     * @param trace the metrics of the operation
     * @deprecated Prefer the use of {@link #startTrace(OperationTrace)} followed by {@link #endTrace(OperationTrace)}
     */
    @Deprecated
    public void     addTrace(OperationTrace trace) {
        startTrace(trace);
        endTrace(trace);
    }

    /**
     * Record the given trace event
     *
     * @param trace name of the counter
     */
    public abstract void addEvent(EventTrace trace);

    @Deprecated
    @Override
    public final void addTrace(String name, long time, TimeUnit unit) {}

    @Deprecated
    @Override
    public final void addCount(String name, int increment) {}
}
