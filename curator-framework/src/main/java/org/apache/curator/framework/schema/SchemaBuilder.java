package org.apache.curator.framework.schema;

import com.google.common.base.Preconditions;
import java.util.regex.Pattern;

public class SchemaBuilder
{
    private Pattern path;
    private String documentation = "";
    private DataValidator dataValidator = new DefaultDataValidator();
    private Schema.Allowance ephemeral = Schema.Allowance.CAN;
    private Schema.Allowance sequential = Schema.Allowance.CAN;
    private boolean canBeWatched = true;
    private boolean canHaveChildren = true;
    private boolean canBeDeleted = true;

    public Schema build()
    {
        return new Schema(path, documentation, dataValidator, ephemeral, sequential, canBeWatched, canHaveChildren, canBeDeleted);
    }

    public SchemaBuilder documentation(String documentation)
    {
        this.documentation = Preconditions.checkNotNull(documentation, "documentation cannot be null");
        return this;
    }

    public SchemaBuilder dataValidator(DataValidator dataValidator)
    {
        this.dataValidator = Preconditions.checkNotNull(dataValidator, "dataValidator cannot be null");
        return this;
    }

    public SchemaBuilder ephemeral(Schema.Allowance ephemeral)
    {
        this.ephemeral = ephemeral;
        return this;
    }

    public SchemaBuilder sequential(Schema.Allowance sequential)
    {
        this.sequential = sequential;
        return this;
    }

    public SchemaBuilder canBeWatched(boolean canBeWatched)
    {
        this.canBeWatched = canBeWatched;
        return this;
    }

    public SchemaBuilder canHaveChildren(boolean canHaveChildren)
    {
        this.canHaveChildren = canHaveChildren;
        return this;
    }

    public SchemaBuilder canBeDeleted(boolean canBeDeleted)
    {
        this.canBeDeleted = canBeDeleted;
        return this;
    }

    SchemaBuilder(Pattern path)
    {
        this.path = Preconditions.checkNotNull(path, "path cannot be null");
    }
}
