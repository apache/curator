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

package org.apache.curator.x.async.modeled.versioned;

import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.x.async.AsyncStage;
import org.apache.zookeeper.data.Stat;

public interface VersionedModeledFramework<T>
{
    /**
     * @see org.apache.curator.x.async.modeled.ModeledFramework#set(Object)
     */
    AsyncStage<String> set(Versioned<T> model);

    /**
     * @see org.apache.curator.x.async.modeled.ModeledFramework#set(Object, org.apache.zookeeper.data.Stat)
     */
    AsyncStage<String> set(Versioned<T> model, Stat storingStatIn);

    /**
     * @see org.apache.curator.x.async.modeled.ModeledFramework#read()
     */
    AsyncStage<Versioned<T>> read();

    /**
     * @see org.apache.curator.x.async.modeled.ModeledFramework#read(org.apache.zookeeper.data.Stat)
     */
    AsyncStage<Versioned<T>> read(Stat storingStatIn);

    /**
     * @see org.apache.curator.x.async.modeled.ModeledFramework#updateOp(Object)
     */
    AsyncStage<Stat> update(Versioned<T> model);

    /**
     * @see org.apache.curator.x.async.modeled.ModeledFramework#updateOp(Object)
     */
    CuratorOp updateOp(Versioned<T> model);
}
