package org.apache.curator.framework.schema;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class SchemaSet
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Collection<Schema> schemas;
    private final CacheLoader<String, Schema> cacheLoader = new CacheLoader<String, Schema>()
    {
        @Override
        public Schema load(String path) throws Exception
        {
            for ( Schema schema : schemas )
            {
                if ( schema.getPath().matcher(path).matches() )
                {
                    log.debug("path -> {}", schema);
                    return schema;
                }
            }
            return defaultSchema;
        }
    };
    private final LoadingCache<String, Schema> cache = CacheBuilder
        .newBuilder()
        .softValues()
        .build(cacheLoader);

    private static final Schema defaultSchema = new Schema(Pattern.compile(".*"), "__default__", new DefaultDataValidator(), Schema.Allowance.CAN, Schema.Allowance.CAN, true, true);

    public SchemaSet()
    {
        this(Collections.<Schema>emptySet());
    }

    public SchemaSet(Collection<Schema> schemas)
    {
        this.schemas = ImmutableSet.copyOf(Preconditions.checkNotNull(schemas, "schemas cannot be null"));
    }

    public Schema getSchema(String path)
    {
        if ( schemas.size() == 0 )
        {
            return defaultSchema;
        }
        try
        {
            return cache.get(path);
        }
        catch ( ExecutionException e )
        {
            throw new RuntimeException(e);
        }
    }

    public String toDocumentation()
    {
        StringBuilder str = new StringBuilder("Curator Schemas:\n\n");
        for ( Schema schema : schemas )
        {
            str.append(schema).append('\n');
        }
        return str.toString();
    }
}
