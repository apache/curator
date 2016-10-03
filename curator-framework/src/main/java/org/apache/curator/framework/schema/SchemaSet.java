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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Collection of all schemas for a Curator instance
 */
public class SchemaSet
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Map<String, Schema> schemas;
    private final Map<String, Schema> pathToSchemas;
    private final List<Schema> regexSchemas;
    private final CacheLoader<String, Schema> cacheLoader = new CacheLoader<String, Schema>()
    {
        @Override
        public Schema load(String path) throws Exception
        {
            for ( Schema schema : regexSchemas )
            {
                if ( schema.getPathRegex().matcher(path).matches() )
                {
                    log.debug("path -> {}", schema);
                    return schema;
                }
            }
            return nullSchema;
        }
    };
    private final LoadingCache<String, Schema> regexCache = CacheBuilder
        .newBuilder()
        .softValues()
        .build(cacheLoader);

    private static final Schema nullSchema = new Schema("__null__", null, "", "Null schema", new DefaultSchemaValidator(), Schema.Allowance.CAN, Schema.Allowance.CAN, Schema.Allowance.CAN, true, ImmutableMap.<String, String>of());
    private static final Schema defaultSchema = new Schema("__default__", null, "", "Default schema", new DefaultSchemaValidator(), Schema.Allowance.CAN, Schema.Allowance.CAN, Schema.Allowance.CAN, true, ImmutableMap.<String, String>of());
    private final boolean useDefaultSchema;

    /**
     * Return the default (empty) schema set
     *
     * @return default schema set
     */
    public static SchemaSet getDefaultSchemaSet()
    {
        return new SchemaSet(Collections.<Schema>emptyList(), true)
        {
            @Override
            public String toDocumentation()
            {
                return "Default schema";
            }
        };
    }

    /**
     * Define a schema set. Schemas are matched in a well defined order:
     * <ol>
     *     <li>Exact match on full path (i.e. non-regex)</li>
     *     <li>Match on the first regex path, searched in the order given to this constructor</li>
     * </ol>
     *
     * @param schemas the schemas for the set.
     * @param useDefaultSchema if true, return a default schema when there is no match. Otherwise, an exception is thrown
     */
    public SchemaSet(List<Schema> schemas, boolean useDefaultSchema)
    {
        schemas = Preconditions.checkNotNull(schemas, "schemas cannot be null");

        this.useDefaultSchema = useDefaultSchema;
        this.schemas = Maps.uniqueIndex(schemas, new Function<Schema, String>()
        {
            @Override
            public String apply(Schema schema)
            {
                return schema.getName();
            }
        });
        ImmutableMap.Builder<String, Schema> pathBuilder = ImmutableMap.builder();
        ImmutableList.Builder<Schema> regexBuilder = ImmutableList.builder();
        for ( Schema schema : schemas )
        {
            if ( schema.getPath() != null )
            {
                pathBuilder.put(schema.getPath(), schema);
            }
            else
            {
                regexBuilder.add(schema);
            }
        }
        pathToSchemas = pathBuilder.build();
        regexSchemas = regexBuilder.build();
    }

    /**
     * Return the schemas
     *
     * @return schemas
     */
    public Collection<Schema> getSchemas()
    {
        return schemas.values();
    }

    /**
     * Find the first matching schema for the path and return it
     *
     * @param path ZNode full path
     * @return matching schema or a default schema
     */
    public Schema getSchema(String path)
    {
        if ( schemas.size() > 0 )
        {
            Schema schema = pathToSchemas.get(path);
            if ( schema == null )
            {
                try
                {
                    schema = regexCache.get(path);
                    if ( schema.equals(nullSchema) )
                    {
                        schema = useDefaultSchema ? defaultSchema : null;
                    }
                }
                catch ( ExecutionException e )
                {
                    throw new RuntimeException(e);
                }
            }
            if ( schema != null )
            {
                return schema;
            }
        }
        if ( useDefaultSchema )
        {
            return defaultSchema;
        }
        throw new SchemaViolation(null, new SchemaViolation.ViolatorData(path, null, null), "No schema found for: " + path);
    }

    /**
     * Utility to return a ZNode path for the given name
     *
     * @param client Curator client
     * @param name path/schema name
     * @return ZNode path
     */
    public static String getNamedPath(CuratorFramework client, String name)
    {
        return client.getSchemaSet().getNamedSchema(name).getRawPath();
    }

    /**
     * Return the schema with the given key/name
     *
     * @param name name
     * @return schema or null
     */
    public Schema getNamedSchema(String name)
    {
        return schemas.get(name);
    }

    /**
     * Build a user displayable documentation string for the schemas in this set
     *
     * @return documentation
     */
    public String toDocumentation()
    {
        StringBuilder str = new StringBuilder("Curator Schemas:\n\n");
        for ( Map.Entry<String, Schema> schemaEntry : schemas.entrySet() )
        {
            str.append(schemaEntry.getKey()).append('\n').append(schemaEntry.getValue().toDocumentation()).append('\n');
        }
        return str.toString();
    }
}
