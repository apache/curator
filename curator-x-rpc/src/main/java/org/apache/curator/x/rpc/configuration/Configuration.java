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
package org.apache.curator.x.rpc.configuration;

import com.facebook.swift.service.ThriftServerConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.airlift.units.Duration;
import io.dropwizard.logging.LoggingFactory;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Configuration
{
    private ThriftServerConfig thrift = new ThriftServerConfig();
    private LoggingFactory logging = new LoggingFactory();
    private Duration projectionExpiration = new Duration(3, TimeUnit.MINUTES);
    private Duration pingTime = new Duration(5, TimeUnit.SECONDS);
    private List<ConnectionConfiguration> connections = Lists.newArrayList();

    public LoggingFactory getLogging()
    {
        return logging;
    }

    public void setLogging(LoggingFactory logging)
    {
        this.logging = logging;
    }

    public ThriftServerConfig getThrift()
    {
        return thrift;
    }

    public void setThrift(ThriftServerConfig thrift)
    {
        this.thrift = thrift;
    }

    public Duration getProjectionExpiration()
    {
        return projectionExpiration;
    }

    public void setProjectionExpiration(Duration projectionExpiration)
    {
        this.projectionExpiration = projectionExpiration;
    }

    public Duration getPingTime()
    {
        return pingTime;
    }

    public void setPingTime(Duration pingTime)
    {
        this.pingTime = pingTime;
    }

    public List<ConnectionConfiguration> getConnections()
    {
        return ImmutableList.copyOf(connections);
    }

    public void setConnections(List<ConnectionConfiguration> connections)
    {
        this.connections = connections;
    }
}
