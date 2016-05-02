package org.apache.curator.framework.schema;

import com.google.common.base.Preconditions;
import java.util.regex.Pattern;

public class SchemaBuilder
{
    private final Pattern pathRegex;
    private final String path;
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
        return new Schema(pathRegex, path, documentation, dataValidator, ephemeral, sequential, watched, canBeDeleted);
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
