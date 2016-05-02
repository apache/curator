package org.apache.curator.framework.schema;

import com.google.common.base.Preconditions;
import java.util.regex.Pattern;

public class SchemaBuilder
{
    private Pattern path;
    private String documentation = "";
    private DataValidator dataValidator = new DefaultDataValidator();
    private boolean isEphemeral = false;
    private boolean isSequential = false;
    private boolean canBeWatched = true;
    private boolean canHaveChildren = true;

    public Schema build()
    {
        return new Schema(path, documentation, dataValidator, isEphemeral, isSequential, canBeWatched, canHaveChildren);
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

    public SchemaBuilder isEphemeral(boolean isEphemeral)
    {
        this.isEphemeral = isEphemeral;
        return this;
    }

    public SchemaBuilder isSequential(boolean isSequential)
    {
        this.isSequential = isSequential;
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

    SchemaBuilder(Pattern path)
    {
        this.path = Preconditions.checkNotNull(path, "path cannot be null");
    }
}
