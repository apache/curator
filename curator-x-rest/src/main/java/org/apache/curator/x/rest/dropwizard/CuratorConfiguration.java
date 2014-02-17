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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.concurrent.TimeUnit;

public class CuratorConfiguration extends Configuration
{
    @Valid
    @NotNull
    @JsonProperty("connection-string")
    private String zooKeeperConnectionString = "localhost:2181";

    @Valid
    @Min(1)
    @JsonProperty("session-length-ms")
    private int sessionLengthMs = (int)TimeUnit.MINUTES.toMillis(1);

    @Valid
    @Min(1)
    @JsonProperty("connection-timeout-ms")
    private int connectionTimeoutMs = (int)TimeUnit.SECONDS.toMillis(15);

    @Valid
    @Min(1)
    @JsonProperty("retry-base-sleep-ms")
    private int retryBaseSleepMs = 100;

    @Valid
    @Min(1)
    @JsonProperty("retry-qty")
    private int retryQty = 3;

    public String getZooKeeperConnectionString()
    {
        return zooKeeperConnectionString;
    }

    public void setZooKeeperConnectionString(String zooKeeperConnectionString)
    {
        this.zooKeeperConnectionString = zooKeeperConnectionString;
    }

    public int getSessionLengthMs()
    {
        return sessionLengthMs;
    }

    public void setSessionLengthMs(int sessionLengthMs)
    {
        this.sessionLengthMs = sessionLengthMs;
    }

    public int getConnectionTimeoutMs()
    {
        return connectionTimeoutMs;
    }

    public void setConnectionTimeoutMs(int connectionTimeoutMs)
    {
        this.connectionTimeoutMs = connectionTimeoutMs;
    }

    public int getRetryBaseSleepMs()
    {
        return retryBaseSleepMs;
    }

    public void setRetryBaseSleepMs(int retryBaseSleepMs)
    {
        this.retryBaseSleepMs = retryBaseSleepMs;
    }

    public int getRetryQty()
    {
        return retryQty;
    }

    public void setRetryQty(int retryQty)
    {
        this.retryQty = retryQty;
    }
}
