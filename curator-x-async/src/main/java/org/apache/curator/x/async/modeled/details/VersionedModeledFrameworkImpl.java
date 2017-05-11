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
package org.apache.curator.x.async.modeled.details;

import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.x.async.AsyncStage;
import org.apache.curator.x.async.modeled.ModeledFramework;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.apache.curator.x.async.modeled.versioned.VersionedModeledFramework;
import org.apache.zookeeper.data.Stat;

class VersionedModeledFrameworkImpl<T> implements VersionedModeledFramework<T>
{
    private final ModeledFramework<T> client;

    VersionedModeledFrameworkImpl(ModeledFramework<T> client)
    {
        this.client = client;
    }

    @Override
    public AsyncStage<String> set(Versioned<T> model)
    {
        return client.set(model.model(), model.version());
    }

    @Override
    public AsyncStage<String> set(Versioned<T> model, Stat storingStatIn)
    {
        return client.set(model.model(), storingStatIn, model.version());
    }

    @Override
    public AsyncStage<Versioned<T>> read()
    {
        return read(null);
    }

    @Override
    public AsyncStage<Versioned<T>> read(Stat storingStatIn)
    {
        Stat localStat = (storingStatIn != null) ? storingStatIn : new Stat();
        AsyncStage<T> stage = client.read(localStat);
        ModelStage<Versioned<T>> modelStage = ModelStage.make(stage.event());
        stage.whenComplete((model, e) -> {
           if ( e != null )
           {
               modelStage.completeExceptionally(e);
           }
           else
           {
               modelStage.complete(Versioned.from(model, localStat.getVersion()));
           }
        });
        return modelStage;
    }

    @Override
    public AsyncStage<Stat> update(Versioned<T> model)
    {
        return client.update(model.model(), model.version());
    }

    @Override
    public CuratorOp updateOp(Versioned<T> model)
    {
        return client.updateOp(model.model(), model.version());
    }
}
