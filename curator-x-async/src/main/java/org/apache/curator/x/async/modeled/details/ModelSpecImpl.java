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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.curator.framework.schema.Schema;
import org.apache.curator.framework.schema.SchemaValidator;
import org.apache.curator.framework.schema.SchemaViolation;
import org.apache.curator.x.async.api.CreateOption;
import org.apache.curator.x.async.api.DeleteOption;
import org.apache.curator.x.async.modeled.ModelSerializer;
import org.apache.curator.x.async.modeled.ModelSpec;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class ModelSpecImpl<T> implements ModelSpec<T>, SchemaValidator
{
    private final ZPath path;
    private final ModelSerializer<T> serializer;
    private final CreateMode createMode;
    private final List<ACL> aclList;
    private final Set<CreateOption> createOptions;
    private final Set<DeleteOption> deleteOptions;
    private final long ttl;
    private final AtomicReference<Schema> schema = new AtomicReference<>();

    public ModelSpecImpl(ZPath path, ModelSerializer<T> serializer, CreateMode createMode, List<ACL> aclList, Set<CreateOption> createOptions, Set<DeleteOption> deleteOptions, long ttl)
    {
        this.path = Objects.requireNonNull(path, "path cannot be null");
        this.serializer = Objects.requireNonNull(serializer, "serializer cannot be null");
        this.createMode = Objects.requireNonNull(createMode, "createMode cannot be null");
        this.aclList = ImmutableList.copyOf(Objects.requireNonNull(aclList, "aclList cannot be null"));
        this.createOptions = ImmutableSet.copyOf(Objects.requireNonNull(createOptions, "createOptions cannot be null"));
        this.deleteOptions = ImmutableSet.copyOf(Objects.requireNonNull(deleteOptions, "deleteOptions cannot be null"));
        this.ttl = ttl;
    }

    @Override
    public ModelSpec<T> at(Object child)
    {
        return withPath(path.at(child));
    }

    @Override
    public ModelSpec<T> resolved(Object... parameters)
    {
        return withPath(path.resolved(parameters));
    }

    @Override
    public ModelSpec<T> resolved(List<Object> parameters)
    {
        return withPath(path.resolved(parameters));
    }

    @Override
    public ModelSpec<T> withPath(ZPath newPath)
    {
        return new ModelSpecImpl<>(newPath, serializer, createMode, aclList, createOptions, deleteOptions, ttl);
    }

    @Override
    public ZPath path()
    {
        return path;
    }

    @Override
    public ModelSerializer<T> serializer()
    {
        return serializer;
    }

    @Override
    public CreateMode createMode()
    {
        return createMode;
    }

    @Override
    public List<ACL> aclList()
    {
        return aclList;
    }

    @Override
    public Set<CreateOption> createOptions()
    {
        return createOptions;
    }

    @Override
    public Set<DeleteOption> deleteOptions()
    {
        return deleteOptions;
    }

    @Override
    public long ttl()
    {
        return ttl;
    }

    @Override
    public Schema schema()
    {
        Schema schemaValue = schema.get();
        if ( schemaValue == null )
        {
            schemaValue = makeSchema();
            schema.compareAndSet(null, schemaValue);
        }
        return schemaValue;
    }

    @Override
    public boolean equals(Object o)
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        ModelSpecImpl<?> modelSpec = (ModelSpecImpl<?>)o;

        if ( ttl != modelSpec.ttl )
        {
            return false;
        }
        if ( !path.equals(modelSpec.path) )
        {
            return false;
        }
        if ( !serializer.equals(modelSpec.serializer) )
        {
            return false;
        }
        if ( createMode != modelSpec.createMode )
        {
            return false;
        }
        if ( !aclList.equals(modelSpec.aclList) )
        {
            return false;
        }
        if ( !createOptions.equals(modelSpec.createOptions) )
        {
            return false;
        }
        //noinspection SimplifiableIfStatement
        if ( !deleteOptions.equals(modelSpec.deleteOptions) )
        {
            return false;
        }
        return schema.equals(modelSpec.schema);
    }

    @Override
    public int hashCode()
    {
        int result = path.hashCode();
        result = 31 * result + serializer.hashCode();
        result = 31 * result + createMode.hashCode();
        result = 31 * result + aclList.hashCode();
        result = 31 * result + createOptions.hashCode();
        result = 31 * result + deleteOptions.hashCode();
        result = 31 * result + (int)(ttl ^ (ttl >>> 32));
        result = 31 * result + schema.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "ModelSpecImpl{" + "path=" + path + ", serializer=" + serializer + ", createMode=" + createMode + ", aclList=" + aclList + ", createOptions=" + createOptions + ", deleteOptions=" + deleteOptions + ", ttl=" + ttl + ", schema=" + schema + '}';
    }

    @Override
    public boolean isValid(Schema schema, String path, byte[] data, List<ACL> acl)
    {
        if ( !acl.equals(aclList) )
        {
            throw new SchemaViolation(schema, new SchemaViolation.ViolatorData(path, data, acl), "ACLs do not match model ACLs");
        }

        try
        {
            serializer.deserialize(data);
        }
        catch ( RuntimeException e )
        {
            throw new SchemaViolation(schema, new SchemaViolation.ViolatorData(path, data, acl), "Data cannot be deserialized into a model");
        }
        return true;
    }

    private Schema makeSchema()
    {
        return Schema.builder(path.toSchemaPathPattern())
            .dataValidator(this)
            .ephemeral(createMode.isEphemeral() ? Schema.Allowance.MUST : Schema.Allowance.CANNOT)
            .canBeDeleted(true)
            .sequential(createMode.isSequential() ? Schema.Allowance.MUST : Schema.Allowance.CANNOT)
            .watched(Schema.Allowance.CAN)
            .build();
    }
}
