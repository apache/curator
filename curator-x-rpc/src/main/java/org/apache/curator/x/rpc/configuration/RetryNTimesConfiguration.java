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

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.airlift.units.Duration;
import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.RetryNTimes;
import java.util.concurrent.TimeUnit;

@JsonTypeName("ntimes")
public class RetryNTimesConfiguration extends RetryPolicyConfiguration
{
    private Duration sleepBetweenRetries = new Duration(100, TimeUnit.MILLISECONDS);
    private int n = 3;

    @Override
    public RetryPolicy build()
    {
        return new RetryNTimes(n, (int)sleepBetweenRetries.toMillis());
    }

    public Duration getSleepBetweenRetries()
    {
        return sleepBetweenRetries;
    }

    public void setSleepBetweenRetries(Duration sleepBetweenRetries)
    {
        this.sleepBetweenRetries = sleepBetweenRetries;
    }

    public int getN()
    {
        return n;
    }

    public void setN(int n)
    {
        this.n = n;
    }
}
