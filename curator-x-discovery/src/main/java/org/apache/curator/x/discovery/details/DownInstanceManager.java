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

package org.apache.curator.x.discovery.details;

import com.google.common.collect.Maps;

import org.apache.curator.x.discovery.DownInstancePolicy;
import org.apache.curator.x.discovery.InstanceFilter;
import org.apache.curator.x.discovery.ServiceInstance;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class DownInstanceManager<T> implements InstanceFilter<T>
{
    private final ConcurrentMap<ServiceInstance<?>, Status> statuses = Maps.newConcurrentMap();
    private final DownInstancePolicy downInstancePolicy;
    private final AtomicLong lastPurge = new AtomicLong(System.currentTimeMillis());

    private static class Status
    {
        private final long startMs = System.currentTimeMillis();
        private final AtomicInteger errorCount = new AtomicInteger(0);
    }

    DownInstanceManager(DownInstancePolicy downInstancePolicy)
    {
        this.downInstancePolicy = downInstancePolicy;
    }

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
        return (status == null) || (status.errorCount.get() < downInstancePolicy.getErrorThreshold());
    }

    private void purge()
    {
        long localLastPurge = lastPurge.get();
        long ticksSinceLastPurge = System.currentTimeMillis() - localLastPurge;
        if ( ticksSinceLastPurge < (downInstancePolicy.getTimeoutMs() / 2) )
        {
            return;
        }

        if ( !lastPurge.compareAndSet(localLastPurge, System.currentTimeMillis()) )
        {
            return;
        }

        Iterator<Entry<ServiceInstance<?>, Status>> it = statuses.entrySet().iterator();
        while ( it.hasNext() )
        {
            Entry<ServiceInstance<?>, Status> entry = it.next();
            long elapsedMs = System.currentTimeMillis() - entry.getValue().startMs;
            if ( elapsedMs >= downInstancePolicy.getTimeoutMs() )
            {
                it.remove();
            }
        }
    }
}
