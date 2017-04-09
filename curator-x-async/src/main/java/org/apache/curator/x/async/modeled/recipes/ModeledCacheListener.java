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
package org.apache.curator.x.async.modeled.recipes;

import java.util.function.Predicate;

/**
 * Event listener
 */
@FunctionalInterface
public interface ModeledCacheListener<T>
{
    /**
     * Receive an event
     *
     * @param event the event
     */
    void event(ModeledCacheEvent<T> event);

    /**
     * Wrap this listener with a filter
     *
     * @param filter test for events. Only events that pass the filter are sent to the listener
     * @return filtered version of this listener
     */
    static <T> ModeledCacheListener<T> filtered(ModeledCacheListener<T> listener, Predicate<ModeledCacheEvent<T>> filter)
    {
        return event -> {
            if ( filter.test(event) )
            {
                listener.event(event);
            }
        };
    }

    /**
     * Filters out all but CRUD events
     *
     * @return predicate
     */
    static <T> Predicate<ModeledCacheEvent<T>> nodeEventFilter()
    {
        return event -> (event.getType() == ModeledCacheEventType.NODE_ADDED)
            || (event.getType() == ModeledCacheEventType.NODE_UPDATED)
            || (event.getType() == ModeledCacheEventType.NODE_REMOVED)
            ;
    }

    /**
     * Filters out all but {@link ModeledCacheEventType#NODE_REMOVED} events
     *
     * @return predicate
     */
    static <T> Predicate<ModeledCacheEvent<T>> nodeRemovedFilter()
    {
        return event -> event.getType() == ModeledCacheEventType.NODE_REMOVED;
    }

    /**
     * Filters out all but events that have valid model instances
     *
     * @return predicate
     */
    static <T> Predicate<ModeledCacheEvent<T>> hasModelFilter()
    {
        return event -> event.getNode().isPresent() && (event.getNode().get().getModel() != null);
    }
}
