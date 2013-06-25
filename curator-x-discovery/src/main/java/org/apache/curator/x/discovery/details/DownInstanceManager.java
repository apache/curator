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
package org.apache.curator.x.discovery.details;

import com.google.common.collect.Maps;
import org.apache.curator.x.discovery.InstanceFilter;
import org.apache.curator.x.discovery.ServiceInstance;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class DownInstanceManager<T> implements InstanceFilter<T>
{
    private final long timeoutMs;
    private final int threshold;
    private final ConcurrentMap<ServiceInstance<?>, Status> statuses = Maps.newConcurrentMap();

    private static final long DEFAULT_TIMEOUT_MS = 30000;
    private static final int DEFAULT_THRESHOLD = 2;

    private static class Status
    {
        private final long startMs = System.currentTimeMillis();
        private final AtomicInteger errorCount = new AtomicInteger(0);
    }

    DownInstanceManager()
    {
        this(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS, DEFAULT_THRESHOLD);
    }

    /**
     * @param timeout amount of time for instances to be unavailable
     * @param unit time unit
     */
    DownInstanceManager(long timeout, TimeUnit unit, int threshold)
    {
        this.threshold = threshold;
        timeoutMs = unit.toMillis(timeout);
    }

    /**
     * This instance will be unavailable for the configured timeout
     *
     * @param instance instance to mark unavailable
     */
    void add(ServiceInstance<?> instance)
    {
        purge();

        Status newStatus = new Status();
        Status oldStatus = statuses.putIfAbsent(instance, newStatus);
        Status useStatus = (oldStatus != null) ? oldStatus : newStatus;
        useStatus.errorCount.incrementAndGet();
    }

    @Override
    public boolean apply(ServiceInstance<T> instance)
    {
        purge();

        Status status = statuses.get(instance);
        return (status == null) || (status.errorCount.get() < threshold);
    }

    private void purge()
    {
        for ( Map.Entry<ServiceInstance<?>, Status> entry : statuses.entrySet() )
        {
            long elapsedMs = System.currentTimeMillis() - entry.getValue().startMs;
            if ( elapsedMs >= timeoutMs )
            {
                statuses.remove(entry.getKey());
            }
        }
    }
}
