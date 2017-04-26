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
import org.apache.curator.x.async.modeled.CuratorModelSpec;
import org.apache.curator.x.async.modeled.ModelSerializer;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

public class CuratorModelSpecImpl<T> implements CuratorModelSpec<T>, SchemaValidator
{
    private final ZPath path;
    private final ModelSerializer<T> serializer;
    private final CreateMode createMode;
    private final List<ACL> aclList;
    private final Set<CreateOption> createOptions;
    private final Set<DeleteOption> deleteOptions;
    private final Schema schema;

    public CuratorModelSpecImpl(ZPath path, ModelSerializer<T> serializer, CreateMode createMode, List<ACL> aclList, Set<CreateOption> createOptions, Set<DeleteOption> deleteOptions)
    {
        this(path, serializer, createMode, aclList, createOptions, deleteOptions, null);
    }

    private CuratorModelSpecImpl(ZPath path, ModelSerializer<T> serializer, CreateMode createMode, List<ACL> aclList, Set<CreateOption> createOptions, Set<DeleteOption> deleteOptions, Schema schema)
    {
        this.path = Objects.requireNonNull(path, "path cannot be null");
        this.serializer = Objects.requireNonNull(serializer, "serializer cannot be null");
        this.createMode = Objects.requireNonNull(createMode, "createMode cannot be null");
        this.aclList = ImmutableList.copyOf(Objects.requireNonNull(aclList, "aclList cannot be null"));
        this.createOptions = ImmutableSet.copyOf(Objects.requireNonNull(createOptions, "createOptions cannot be null"));
        this.deleteOptions = ImmutableSet.copyOf(Objects.requireNonNull(deleteOptions, "deleteOptions cannot be null"));

        this.schema = (schema != null) ? schema : makeSchema(); // must be last in statement in ctor
    }

    @Override
    public CuratorModelSpec<T> at(String child)
    {
        return new CuratorModelSpecImpl<>(path.at(child), serializer, createMode, aclList, createOptions, deleteOptions);
    }

    @Override
    public CuratorModelSpec<T> resolved(Object... parameters)
    {
        return new CuratorModelSpecImpl<>(path.resolved(parameters), serializer, createMode, aclList, createOptions, deleteOptions);
    }

    @Override
    public CuratorModelSpec<T> resolved(List<Object> parameters)
    {
        return new CuratorModelSpecImpl<>(path.resolved(parameters), serializer, createMode, aclList, createOptions, deleteOptions);
    }

    @Override
    public CuratorModelSpec<T> resolving(List<Supplier<Object>> parameterSuppliers)
    {
        return new CuratorModelSpecImpl<>(path.resolving(parameterSuppliers), serializer, createMode, aclList, createOptions, deleteOptions);
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
    public Schema schema()
    {
        return schema;
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

        CuratorModelSpecImpl<?> that = (CuratorModelSpecImpl<?>)o;

        if ( !path.equals(that.path) )
        {
            return false;
        }
        if ( !serializer.equals(that.serializer) )
        {
            return false;
        }
        if ( createMode != that.createMode )
        {
            return false;
        }
        if ( !aclList.equals(that.aclList) )
        {
            return false;
        }
        if ( !createOptions.equals(that.createOptions) )
        {
            return false;
        }
        //noinspection SimplifiableIfStatement
        if ( !deleteOptions.equals(that.deleteOptions) )
        {
            return false;
        }
        return schema.equals(that.schema);
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
        result = 31 * result + schema.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "CuratorModelImpl{" + "path=" + path + ", serializer=" + serializer + ", createMode=" + createMode + ", aclList=" + aclList + ", createOptions=" + createOptions + ", deleteOptions=" + deleteOptions + ", schema=" + schema + '}';
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
