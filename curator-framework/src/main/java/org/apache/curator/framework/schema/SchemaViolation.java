package org.apache.curator.framework.schema;

public class SchemaViolation extends RuntimeException
{
    public SchemaViolation(Schema schema, String violation)
    {
        super(String.format("Schema violation: %s for schema: %s", violation, schema));
    }
}
