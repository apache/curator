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

package com.netflix.curator.x.discovery.entities;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.netflix.curator.x.discovery.ServiceInstance;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Collection;
import java.util.List;

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
