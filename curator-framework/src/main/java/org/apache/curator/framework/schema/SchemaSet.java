package org.apache.curator.framework.schema;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Collection of all schemas for a Curator instance
 */
public class SchemaSet
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Map<SchemaKey, Schema> schemas;
    private final Map<String, Schema> pathSchemas;
    private final CacheLoader<String, Schema> cacheLoader = new CacheLoader<String, Schema>()
    {
        @Override
        public Schema load(String path) throws Exception
        {
            for ( Schema schema : schemas.values() )
            {
                if ( (schema.getPathRegex() != null) && schema.getPathRegex().matcher(path).matches() )
                {
                    log.debug("path -> {}", schema);
                    return schema;
                }
            }
            return defaultSchema;
        }
    };
    private final LoadingCache<String, Schema> regexCache = CacheBuilder
        .newBuilder()
        .softValues()
        .build(cacheLoader);

    private static final Schema defaultSchema = new Schema(null, "", "Default schema", new DefaultDataValidator(), Schema.Allowance.CAN, Schema.Allowance.CAN, Schema.Allowance.CAN, true);
    private final boolean useDefaultSchema;

    /**
     * Return the default (empty) schema set
     *
     * @return default schema set
     */
    public static SchemaSet getDefaultSchemaSet()
    {
        return new SchemaSet(Collections.<SchemaKey, Schema>emptyMap(), true)
        {
            @Override
            public String toDocumentation()
            {
                return "Default schema";
            }
        };
    }

    /**
     * @param schemas the schemas for the set. The key of the map is a key/name for the schema that can be
     *                used when calling {@link #getNamedSchema(SchemaKey)}
     * @param useDefaultSchema if true, return a default schema when there is no match. Otherwise, an exception is thrown
     */
    public SchemaSet(Map<SchemaKey, Schema> schemas, boolean useDefaultSchema)
    {
        this.useDefaultSchema = useDefaultSchema;
        this.schemas = ImmutableMap.copyOf(Preconditions.checkNotNull(schemas, "schemas cannot be null"));
        ImmutableMap.Builder<String, Schema> builder = ImmutableMap.builder();
        for ( Schema schema : schemas.values() )
        {
            if ( schema.getPath() != null )
            {
                builder.put(schema.getPath(), schema);
            }
        }
        pathSchemas = builder.build();
    }

    /**
     * Find the first matching schema for the path and return it
     *
     * @param path ZNode full path
     * @return matching schema or a default schema
     */
    public Schema getSchema(String path)
    {
        Schema schema = null;
        if ( schemas.size() > 0 )
        {
            schema = pathSchemas.get(path);
            if ( schema == null )
            {
                try
                {
                    schema = regexCache.get(path);
                }
                catch ( ExecutionException e )
                {
                    throw new RuntimeException(e);
                }
            }
        }
        if ( schema != null )
        {
            return schema;
        }
        if ( useDefaultSchema )
        {
            return defaultSchema;
        }
        throw new SchemaViolation("No schema found for: " + path);
    }

    /**
     * Return the schema with the given key/name
     *
     * @param name name
     * @return schema or null
     */
    public Schema getNamedSchema(SchemaKey name)
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
        for ( Map.Entry<SchemaKey, Schema> schemaEntry : schemas.entrySet() )
        {
            str.append(schemaEntry.getKey()).append('\n').append(schemaEntry.getValue().toDocumentation()).append('\n');
        }
        return str.toString();
    }
}
