package org.apache.curator.framework.schema;

/**
 * Thrown by the various <code>validation</code> methods in a Schema
 */
public class SchemaViolation extends RuntimeException
{
    private final Schema schema;
    private final String violation;

    public SchemaViolation(Schema schema, String violation)
    {
        super(String.format("Schema violation: %s for schema: %s", violation, schema));
        this.schema = schema;
        this.violation = violation;
    }

    public Schema getSchema()
    {
        return schema;
    }

    public String getViolation()
    {
        return violation;
    }
}
