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
package org.apache.curator.framework.schema;

import com.google.common.base.Preconditions;
import org.apache.zookeeper.CreateMode;
import java.util.regex.Pattern;

/**
 * Represents and documents operations allowed for a given path pattern
 */
public class Schema
{
    private final String name;
    private final Pattern pathRegex;
    private final String path;
    private final String documentation;
    private final DataValidator dataValidator;
    private final Allowance ephemeral;
    private final Allowance sequential;
    private final Schema.Allowance watched;
    private final boolean canBeDeleted;

    public enum Allowance
    {
        CAN,
        MUST,
        CANNOT
    }

    /**
     * Start a builder for the given full path. Note: full path schemas
     * take precedence over regex path schemas.
     *
     * @param path full ZNode path. This schema only applies to an exact match
     * @return builder
     */
    public static SchemaBuilder builder(String path)
    {
        return new SchemaBuilder(null, path);
    }

    /**
     * Start a builder for the given path pattern.
     *
     * @param pathRegex regex for the path. This schema applies to any matching paths
     * @return builder
     */
    public static SchemaBuilder builder(Pattern pathRegex)
    {
        return new SchemaBuilder(pathRegex, null);
    }

    Schema(String name, Pattern pathRegex, String path, String documentation, DataValidator dataValidator, Allowance ephemeral, Allowance sequential, Allowance watched, boolean canBeDeleted)
    {
        Preconditions.checkNotNull((pathRegex != null) || (path != null), "pathRegex and path cannot both be null");
        this.pathRegex = pathRegex;
        this.path = path;
        this.name = Preconditions.checkNotNull(name, "name cannot be null");
        this.documentation = Preconditions.checkNotNull(documentation, "documentation cannot be null");
        this.dataValidator = Preconditions.checkNotNull(dataValidator, "dataValidator cannot be null");
        this.ephemeral = Preconditions.checkNotNull(ephemeral, "ephemeral cannot be null");
        this.sequential = Preconditions.checkNotNull(sequential, "sequential cannot be null");
        this.watched = Preconditions.checkNotNull(watched, "watched cannot be null");
        this.canBeDeleted = canBeDeleted;
    }

    /**
     * Validate that this schema allows znode deletion
     *
     * @throws SchemaViolation if schema does not allow znode deletion
     */
    public void validateDeletion()
    {
        if ( !canBeDeleted )
        {
            throw new SchemaViolation(this, "Cannot be deleted");
        }
    }

    /**
     * Validate that this schema's watching setting matches
     *
     * @param isWatching true if attempt is being made to watch node
     * @throws SchemaViolation if schema's watching setting does not match
     */
    public void validateWatcher(boolean isWatching)
    {
        if ( isWatching && (watched == Allowance.CANNOT) )
        {
            throw new SchemaViolation(this, "Cannot be watched");
        }

        if ( !isWatching && (watched == Allowance.MUST) )
        {
            throw new SchemaViolation(this, "Must be watched");
        }
    }

    /**
     * Validate that this schema's create mode setting matches and that the data is valid
     *
     * @param mode CreateMode being used
     * @param data data being set
     * @throws SchemaViolation if schema's create mode setting does not match or data is invalid
     */
    public void validateCreate(CreateMode mode, byte[] data)
    {
        if ( mode.isEphemeral() && (ephemeral == Allowance.CANNOT) )
        {
            throw new SchemaViolation(this, "Cannot be ephemeral");
        }

        if ( !mode.isEphemeral() && (ephemeral == Allowance.MUST) )
        {
            throw new SchemaViolation(this, "Must be ephemeral");
        }

        if ( mode.isSequential() && (sequential == Allowance.CANNOT) )
        {
            throw new SchemaViolation(this, "Cannot be sequential");
        }

        if ( !mode.isSequential() && (sequential == Allowance.MUST) )
        {
            throw new SchemaViolation(this, "Must be sequential");
        }

        validateData(path, data);
    }

    /**
     * Validate that this schema validates the data
     *
     *
     * @param path the znode full path
     * @param data data being set
     * @throws SchemaViolation if data is invalid
     */
    public void validateData(String path, byte[] data)
    {
        if ( !dataValidator.isValid(path, data) )
        {
            throw new SchemaViolation(this, "Data is not valid");
        }
    }

    public String getName()
    {
        return name;
    }

    /**
     * Return the raw path for this schema. If a full path was used, it is returned.
     * If a regex was used, it is returned
     *
     * @return path
     */
    public String getRawPath()
    {
        return (path != null) ? path : pathRegex.pattern();
    }

    Pattern getPathRegex()
    {
        return pathRegex;
    }

    String getPath()
    {
        return path;
    }

    // intentionally only path and pathRegex
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

        Schema schema = (Schema)o;

        //noinspection SimplifiableIfStatement
        if ( !pathRegex.equals(schema.pathRegex) )
        {
            return false;
        }
        return path.equals(schema.path);

    }

    // intentionally only path and pathRegex
    @Override
    public int hashCode()
    {
        int result = pathRegex.hashCode();
        result = 31 * result + path.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "Schema{" +
            "name='" + name + '\'' +
            ", pathRegex=" + pathRegex +
            ", path='" + path + '\'' +
            ", documentation='" + documentation + '\'' +
            ", dataValidator=" + dataValidator +
            ", ephemeral=" + ephemeral +
            ", sequential=" + sequential +
            ", watched=" + watched +
            ", canBeDeleted=" + canBeDeleted +
            '}';
    }

    public String toDocumentation()
    {
        return "Name: " + name + '\n'
            + "Path: " + getRawPath() + '\n'
            + "Documentation: " + documentation + '\n'
            + "Validator: " + dataValidator.getClass().getSimpleName() + '\n'
            + String.format("ephemeral: %s | sequential: %s | watched: %s | canBeDeleted: %s", ephemeral, sequential, watched, canBeDeleted) + '\n'
            ;
    }
}
