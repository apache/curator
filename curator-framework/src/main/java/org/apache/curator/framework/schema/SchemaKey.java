package org.apache.curator.framework.schema;

import com.google.common.base.Preconditions;
import java.util.UUID;

public class SchemaKey
{
    private final String key;

    public static SchemaKey named(String name)
    {
        return new SchemaKey(name);
    }

    public SchemaKey()
    {
        this(UUID.randomUUID().toString());
    }

    public SchemaKey(String key)
    {
        this.key = Preconditions.checkNotNull(key, "key cannot be null");
    }

    public String getKey()
    {
        return key;
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

        SchemaKey schemaKey = (SchemaKey)o;

        return key.equals(schemaKey.key);

    }

    @Override
    public int hashCode()
    {
        return key.hashCode();
    }

    @Override
    public String toString()
    {
        return key;
    }
}
