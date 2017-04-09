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
package modeled;

import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.x.async.modeled.JacksonModelSerializer;
import org.apache.curator.x.async.modeled.recipes.ModeledCacheEventType;
import org.apache.curator.x.async.modeled.recipes.ModeledCacheListener;
import org.apache.curator.x.async.modeled.recipes.ModeledTreeCache;
import java.util.function.Consumer;

public class ModeledCacheExamples
{
    public static ModeledTreeCache<PersonModel> wrap(TreeCache cache)
    {
        JacksonModelSerializer<PersonModel> serializer = JacksonModelSerializer.build(PersonModel.class);

        // wrap a TreeCache instance so that it can be used "modeled".
        return ModeledTreeCache.wrap(cache, serializer);
    }

    public static void watchForChanges(TreeCache cache, Consumer<PersonModel> deletePersonReceiver, Consumer<PersonModel> updatedPersonReceiver)
    {
        ModeledTreeCache<PersonModel> modeledCache = wrap(cache);
        ModeledCacheListener<PersonModel> listener = event -> {
            PersonModel person = event.getNode().getModel();
            if ( event.getType() == ModeledCacheEventType.NODE_REMOVED )
            {
                deletePersonReceiver.accept(person);
            }
            else
            {
                updatedPersonReceiver.accept(person);
            }
        };

        // take a standard listener and filter so that only events that have a valid model instance are sent to the listener
        ModeledCacheListener<PersonModel> filteredListener = ModeledCacheListener.filtered(listener, ModeledCacheListener.hasModelFilter());
        modeledCache.getListenable().addListener(filteredListener);
    }
}
