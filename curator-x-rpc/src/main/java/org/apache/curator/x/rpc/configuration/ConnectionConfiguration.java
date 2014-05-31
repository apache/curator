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

import com.google.common.base.Preconditions;
import io.airlift.units.Duration;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import javax.validation.constraints.NotNull;
import java.util.concurrent.TimeUnit;

public class ConnectionConfiguration
{
    @NotNull private String name;
    private String connectionString = null;
    private Duration sessionLength = new Duration(1, TimeUnit.MINUTES);
    private Duration connectionTimeout = new Duration(15, TimeUnit.SECONDS);
    private AuthorizationConfiguration authorization = null;
    private String namespace = null;
    private RetryPolicyConfiguration retry = new ExponentialBackoffRetryConfiguration();

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getConnectionString()
    {
        return connectionString;
    }

    public void setConnectionString(String connectionString)
    {
        this.connectionString = connectionString;
    }

    public Duration getSessionLength()
    {
        return sessionLength;
    }

    public void setSessionLength(Duration sessionLength)
    {
        this.sessionLength = sessionLength;
    }

    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    public void setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
    }

    public AuthorizationConfiguration getAuthorization()
    {
        return authorization;
    }

    public void setAuthorization(AuthorizationConfiguration authorization)
    {
        this.authorization = authorization;
    }

    public String getNamespace()
    {
        return namespace;
    }

    public void setNamespace(String namespace)
    {
        this.namespace = namespace;
    }

    public RetryPolicyConfiguration getRetry()
    {
        return retry;
    }

    public void setRetry(RetryPolicyConfiguration retry)
    {
        this.retry = retry;
    }

    public CuratorFramework build()
    {
        Preconditions.checkState((connectionString != null) && (connectionString.length() > 0), "You must specify a connection string for connection: " + name);
        Preconditions.checkNotNull(retry, "retry cannot be null");

        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        builder = builder
            .connectString(connectionString)
            .sessionTimeoutMs((int)sessionLength.toMillis())
            .connectionTimeoutMs((int)connectionTimeout.toMillis())
            .retryPolicy(retry.build());
        if ( authorization != null )
        {
            builder = builder.authorization(authorization.getScheme(), authorization.getAuth().getBytes());
        }
        if ( namespace != null )
        {
            builder = builder.namespace(namespace);
        }
        return builder.build();
    }
}
