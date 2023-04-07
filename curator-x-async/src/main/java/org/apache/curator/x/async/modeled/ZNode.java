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

package org.apache.curator.x.async.modeled;

import org.apache.curator.x.async.AsyncStage;
import org.apache.zookeeper.data.Stat;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

/**
 * Abstracts a ZooKeeper node
 */
public interface ZNode<T>
{
    /**
     * The path of the node
     *
     * @return path
     */
    ZPath path();

    /**
     * The node's last known stat if available
     *
     * @return stat
     */
    Stat stat();

    /**
     * The node's current model
     *
     * @return model
     */
    T model();

    /**
     * Utility that modifies an async stage of znodes into an async stage of models
     *
     * @param from original stage
     * @return stage of models
     */
    static <T> CompletionStage<List<T>> models(AsyncStage<List<ZNode<T>>> from)
    {
        return from.thenApply(nodes -> nodes.stream().map(ZNode::model).collect(Collectors.toList()));
    }

    /**
     * Utility that modifies an async stage of a znode into an async stage of a model
     *
     * @param from original stage
     * @return stage of a model
     */
    static <T> CompletionStage<T> model(AsyncStage<ZNode<T>> from)
    {
        return from.thenApply(ZNode::model);
    }
}
