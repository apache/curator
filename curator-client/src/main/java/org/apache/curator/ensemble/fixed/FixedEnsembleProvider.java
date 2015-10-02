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
package org.apache.curator.ensemble.fixed;

import com.google.common.base.Preconditions;
import org.apache.curator.ensemble.EnsembleProvider;
import org.apache.zookeeper.ZooKeeper;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Standard ensemble provider that wraps a fixed connection string
 */
public class FixedEnsembleProvider implements EnsembleProvider
{
    private final AtomicReference<String> connectionString = new AtomicReference<>();
    private final boolean updateServerListEnabled;

    /**
     * The connection string to use
     *
     * @param connectionString connection string
     */
    public FixedEnsembleProvider(String connectionString)
    {
        this(connectionString, true);
    }

    /**
     * The connection string to use
     *
     * @param connectionString connection string
     * @param updateServerListEnabled if true, allow Curator to call {@link ZooKeeper#updateServerList(String)}
     */
    public FixedEnsembleProvider(String connectionString, boolean updateServerListEnabled)
    {
        this.updateServerListEnabled = updateServerListEnabled;
        this.connectionString.set(Preconditions.checkNotNull(connectionString, "connectionString cannot be null"));
    }

    @Override
    public void start() throws Exception
    {
        // NOP
    }

    @Override
    public void close() throws IOException
    {
        // NOP
    }

    @Override
    public void setConnectionString(String connectionString)
    {
        this.connectionString.set(connectionString);
    }

    @Override
    public String getConnectionString()
    {
        return connectionString.get();
    }

    @Override
    public boolean updateServerListEnabled()
    {
        return updateServerListEnabled;
    }
}
