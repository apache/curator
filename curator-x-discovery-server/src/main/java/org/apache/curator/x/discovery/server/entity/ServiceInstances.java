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

package org.apache.curator.x.discovery.server.entity;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.curator.x.discovery.ServiceInstance;
import java.util.Collection;
import java.util.List;

/**
 * Raw generic lists don't work well in JAX-RS. Thus, this wrapper is needed.
 */
public class ServiceInstances<T>
{
    private final List<ServiceInstance<T>> services;

    public ServiceInstances()
    {
        services = Lists.newArrayList();
    }

    public ServiceInstances(Collection<? extends ServiceInstance<T>> c)
    {
        services = Lists.newArrayList(c);
    }

    public List<ServiceInstance<T>> getServices()
    {
        return ImmutableList.copyOf(services);
    }
}
