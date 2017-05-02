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
import org.apache.curator.x.async.modeled.ModelSpec;
import org.apache.curator.x.async.modeled.JacksonModelSerializer;
import org.apache.curator.x.async.modeled.ModeledCuratorFramework;
import org.apache.curator.x.async.modeled.ZPath;

public class PersonModelSpec
{
    private final CuratorFramework client;
    private final ModelSpec<PersonModel> modelSpec;

    public PersonModelSpec(CuratorFramework client)
    {
        this.client = client;

        JacksonModelSerializer<PersonModel> serializer = JacksonModelSerializer.build(PersonModel.class);
        ZPath path = ZPath.from("example", ZPath.parameterNodeName(), "path", ZPath.parameterNodeName());
        modelSpec = ModelSpec.builder(path, serializer).build();
    }

    public ModeledCuratorFramework<PersonModel> resolved(ContainerType containerType, PersonId personId)
    {
        ModelSpec<PersonModel> resolved = null;// TODO modelSpec.resolved(containerType.getTypeId(), personId.getId());
        return ModeledCuratorFramework.wrap(client, resolved);
    }
}
