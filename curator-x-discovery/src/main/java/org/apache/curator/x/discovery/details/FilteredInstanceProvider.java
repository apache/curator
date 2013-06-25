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

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.curator.x.discovery.InstanceFilter;
import org.apache.curator.x.discovery.ServiceInstance;
import java.util.List;

class FilteredInstanceProvider<T> implements InstanceProvider<T>
{
    private final InstanceProvider<T> instanceProvider;
    private final Predicate<ServiceInstance<T>> predicates;

    FilteredInstanceProvider(InstanceProvider<T> instanceProvider, List<InstanceFilter<T>> filters)
    {
        this.instanceProvider = instanceProvider;
        predicates = Predicates.and(filters);
    }

    @Override
    public List<ServiceInstance<T>> getInstances() throws Exception
    {
        Iterable<ServiceInstance<T>> filtered = Iterables.filter(instanceProvider.getInstances(), predicates);
        return ImmutableList.copyOf(filtered);
    }
}
