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

package org.apache.curator.x.rest.dropwizard;

import com.codahale.metrics.health.HealthCheck;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.x.rest.CuratorRestContext;

public class CuratorHealthCheck extends HealthCheck
{
    private final CuratorRestContext context;

    public CuratorHealthCheck(CuratorRestContext context)
    {
        this.context = context;
    }

    @Override
    protected Result check() throws Exception
    {
        ConnectionState state = context.getConnectionState();
        if ( state != ConnectionState.CONNECTED )
        {
            return Result.unhealthy(state.name());
        }
        return Result.healthy();
    }
}
