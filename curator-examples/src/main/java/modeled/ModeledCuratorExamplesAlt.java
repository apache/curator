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

import org.apache.curator.x.async.modeled.ModeledFramework;
import java.util.function.Consumer;

public class ModeledCuratorExamplesAlt
{
    public static void createOrUpdate(PersonModelSpec modelSpec, PersonModel model)
    {
        // change the affected path to be modeled's base path plus id: i.e. "/example/path/{id}"
        ModeledFramework<PersonModel> resolved = modelSpec.resolved(model.getContainerType(), model.getId());

        // by default ModeledFramework instances update the node if it already exists
        // so this will either create or update the node
        resolved.set(model); // note - this is async
    }

    public static void readPerson(PersonModelSpec modelSpec, ContainerType containerType, PersonId id, Consumer<PersonModel> receiver)
    {
        ModeledFramework<PersonModel> resolved = modelSpec.resolved(containerType, id);

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
