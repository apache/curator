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
package org.apache.curator.x.crimps.async;

import org.apache.curator.framework.api.ConfigureEnsembleable;
import org.apache.curator.framework.api.Ensembleable;
import org.apache.curator.framework.api.Statable;
import org.apache.zookeeper.data.Stat;
import java.util.concurrent.CompletionStage;

class CrimpedEnsembleableImpl implements CrimpedEnsembleable
{
    private final CrimpedBackgroundCallback<byte[]> callback;
    private final Statable<ConfigureEnsembleable> configBuilder;
    private Ensembleable<byte[]> ensembleable;
    private ConfigureEnsembleable configureEnsembleable;

    CrimpedEnsembleableImpl(Statable<ConfigureEnsembleable> configBuilder, CrimpedBackgroundCallback<byte[]> callback)
    {
        this.configBuilder = configBuilder;
        this.callback = callback;
        configureEnsembleable = configBuilder.storingStatIn(new Stat());
        ensembleable = configureEnsembleable;
    }

    CrimpedEnsembleableImpl(Ensembleable<byte[]> ensembleable, CrimpedBackgroundCallback<byte[]> callback)
    {
        this.ensembleable = ensembleable;
        this.configBuilder = null;
        this.callback = callback;
        configureEnsembleable = null;
    }

    @Override
    public CompletionStage<byte[]> forEnsemble() throws Exception
    {
        ensembleable.forEnsemble();
        return callback;
    }

    @Override
    public CrimpedConfigEnsembleable storingStatIn(Stat stat)
    {
        ensembleable = configureEnsembleable = configBuilder.storingStatIn(stat);
        return this;
    }

    @Override
    public Ensembleable<CompletionStage<byte[]>> fromConfig(long config) throws Exception
    {
        ensembleable = configureEnsembleable.fromConfig(config);
        return this;
    }
}
