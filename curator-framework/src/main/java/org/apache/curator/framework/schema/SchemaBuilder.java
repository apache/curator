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
import java.util.UUID;
import java.util.regex.Pattern;

public class SchemaBuilder
{
    private final Pattern pathRegex;
    private final String path;
    private String name = UUID.randomUUID().toString();
    private String documentation = "";
    private DataValidator dataValidator = new DefaultDataValidator();
    private Schema.Allowance ephemeral = Schema.Allowance.CAN;
    private Schema.Allowance sequential = Schema.Allowance.CAN;
    private Schema.Allowance watched = Schema.Allowance.CAN;
    private boolean canBeDeleted = true;

    /**
     * Build a new schema from the currently set values
     *
     * @return new schema
     */
    public Schema build()
    {
        return new Schema(name, pathRegex, path, documentation, dataValidator, ephemeral, sequential, watched, canBeDeleted);
    }

    /**
     * @param name unique name for this schema
     * @return this for chaining
     */
    public SchemaBuilder name(String name)
    {
        this.name = Preconditions.checkNotNull(name, "name cannot be null");
        return this;
    }

    /**
     * @param documentation user displayable documentation for the schema
     * @return this for chaining
     */
    public SchemaBuilder documentation(String documentation)
    {
        this.documentation = Preconditions.checkNotNull(documentation, "documentation cannot be null");
        return this;
    }

    /**
     * @param dataValidator a data validator - will be used to validate data set for the znode
     * @return this for chaining
     */
    public SchemaBuilder dataValidator(DataValidator dataValidator)
    {
        this.dataValidator = Preconditions.checkNotNull(dataValidator, "dataValidator cannot be null");
        return this;
    }

    /**
     * @param ephemeral whether can, must or cannot be ephemeral
     * @return this for chaining
     */
    public SchemaBuilder ephemeral(Schema.Allowance ephemeral)
    {
        this.ephemeral = Preconditions.checkNotNull(ephemeral, "ephemeral cannot be null");
        return this;
    }

    /**
     * @param sequential whether can, must or cannot be sequential
     * @return this for chaining
     */
    public SchemaBuilder sequential(Schema.Allowance sequential)
    {
        this.sequential = Preconditions.checkNotNull(sequential, "sequential cannot be null");
        return this;
    }

    /**
     * @param watched whether can, must or cannot be watched
     * @return this for chaining
     */
    public SchemaBuilder watched(Schema.Allowance watched)
    {
        this.watched = watched;
        return this;
    }

    /**
     * @param canBeDeleted true if znode can be deleted
     * @return this for chaining
     */
    public SchemaBuilder canBeDeleted(boolean canBeDeleted)
    {
        this.canBeDeleted = canBeDeleted;
        return this;
    }

    SchemaBuilder(Pattern pathRegex, String path)
    {
        this.pathRegex = pathRegex;
        this.path = path;
    }
}
