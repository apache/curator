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

package org.apache.curator.x.async.modeled.cached;

import org.apache.curator.x.async.modeled.ZPath;
import org.apache.zookeeper.data.Stat;
import org.slf4j.LoggerFactory;

@FunctionalInterface
public interface ModeledCacheListener<T> {
    enum Type {
        /**
         * A child was added to the path
         */
        NODE_ADDED,

        /**
         * A child's data was changed
         */
        NODE_UPDATED,

        /**
         * A child was removed from the path
         */
        NODE_REMOVED
    }

    /**
     * The given path was added, updated or removed
     *
     * @param type action type
     * @param path the path
     * @param stat the node's stat (previous stat for removal)
     * @param model the node's model (previous model for removal)
     */
    void accept(Type type, ZPath path, Stat stat, T model);

    /**
     * The cache has finished initializing
     */
    default void initialized() {
        // NOP
    }

    /**
     * Called when there is an exception processing a message from the internal cache. This is most
     * likely due to a de-serialization problem.
     *
     * @param e the exception
     */
    default void handleException(Exception e) {
        LoggerFactory.getLogger(getClass()).error("Could not process cache message", e);
    }

    /**
     * Returns a version of this listener that only begins calling
     * {@link #accept(org.apache.curator.x.async.modeled.cached.ModeledCacheListener.Type, org.apache.curator.x.async.modeled.ZPath, org.apache.zookeeper.data.Stat, Object)}
     * once {@link #initialized()} has been called. i.e. changes that occur as the cache is initializing are not sent
     * to the listener
     *
     * @return wrapped listener
     */
    default ModeledCacheListener<T> postInitializedOnly() {
        return new ModeledCacheListener<T>() {
            private volatile boolean isInitialized = false;

            @Override
            public void accept(Type type, ZPath path, Stat stat, T model) {
                if (isInitialized) {
                    ModeledCacheListener.this.accept(type, path, stat, model);
                }
            }

            @Override
            public void initialized() {
                isInitialized = true;
                ModeledCacheListener.this.initialized();
            }
        };
    }
}
