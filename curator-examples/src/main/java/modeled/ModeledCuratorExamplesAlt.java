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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.async.modeled.CuratorModelSpec;
import org.apache.curator.x.async.modeled.JacksonModelSerializer;
import org.apache.curator.x.async.modeled.ModeledCuratorFramework;
import org.apache.curator.x.async.modeled.ZPath;
import java.util.function.Consumer;

public class ModeledCuratorExamplesAlt
{
    public static ModeledCuratorFramework<PersonModel> wrap(CuratorFramework client)
    {
        JacksonModelSerializer<PersonModel> serializer = JacksonModelSerializer.build(PersonModel.class);

        ZPath path = ZPath.from("example", ZPath.parameterNodeName(), "path", ZPath.parameterNodeName());

        // build a model specification - you can pre-build all the model specifications for your app at startup
        CuratorModelSpec<PersonModel> modelSpec = CuratorModelSpec.builder(path, serializer).build();

        // wrap a CuratorFramework instance so that it can be used "modeled".
        // do this once and re-use the returned ModeledCuratorFramework instance.
        // ModeledCuratorFramework instances are tied to a given path
        return ModeledCuratorFramework.wrap(client, modelSpec);
    }

    public static void createOrUpdate(ModeledCuratorFramework<PersonModel> modeled, String typeId, PersonModel model)
    {
        // change the affected path to be modeled's base path plus id: i.e. "/example/path/{id}"
        ModeledCuratorFramework<PersonModel> resolved = modeled.resolved(typeId, model.getId());

        // by default ModeledCuratorFramework instances update the node if it already exists
        // so this will either create or update the node
        resolved.create(model); // note - this is async
    }

    public static void readPerson(ModeledCuratorFramework<PersonModel> modeled, String typeId, String id, Consumer<PersonModel> receiver)
    {
        ModeledCuratorFramework<PersonModel> resolved = modeled.resolved(typeId, id);

        // read the person with the given ID and asynchronously call the receiver after it is read
        resolved.read().whenComplete((person, exception) -> {
            if ( exception != null )
            {
                exception.printStackTrace();    // handle the error
            }
            else
            {
                receiver.accept(person);
            }
        });
    }
}
