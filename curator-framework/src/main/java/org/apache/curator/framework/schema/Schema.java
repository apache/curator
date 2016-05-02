package org.apache.curator.framework.schema;

import com.google.common.base.Preconditions;
import org.apache.zookeeper.CreateMode;
import java.util.regex.Pattern;

/**
 * Represents and documents operations allowed for a given path pattern
 */
public class Schema
{
    private final Pattern path;
    private final String documentation;
    private final DataValidator dataValidator;
    private final Allowance ephemeral;
    private final Allowance sequential;
    private final Schema.Allowance watched;
    private final boolean canBeDeleted;

    public enum Allowance
    {
        CAN,
        MUST,
        CANNOT
    }

    /**
     * Start a builder for the given path pattern.
     *
     * @param pathRegex regex for the path. This schema applies to all matching paths
     * @return builder
     */
    public static SchemaBuilder builder(String pathRegex)
    {
        return builder(Pattern.compile(pathRegex));
    }

    /**
     * Start a builder for the given path pattern.
     *
     * @param pathRegex regex for the path. This schema applies to all matching paths
     * @return builder
     */
    public static SchemaBuilder builder(Pattern pathRegex)
    {
        return new SchemaBuilder(pathRegex);
    }

    Schema(Pattern path, String documentation, DataValidator dataValidator, Allowance ephemeral, Allowance sequential, Allowance watched, boolean canBeDeleted)
    {
        this.path = Preconditions.checkNotNull(path, "path cannot be null");
        this.documentation = Preconditions.checkNotNull(documentation, "documentation cannot be null");
        this.dataValidator = Preconditions.checkNotNull(dataValidator, "dataValidator cannot be null");
        this.ephemeral = Preconditions.checkNotNull(ephemeral, "ephemeral cannot be null");
        this.sequential = Preconditions.checkNotNull(sequential, "sequential cannot be null");
        this.watched = Preconditions.checkNotNull(watched, "watched cannot be null");
        this.canBeDeleted = canBeDeleted;
    }

    public void validateDeletion()
    {
        if ( !canBeDeleted )
        {
            throw new SchemaViolation(this, "Cannot be deleted");
        }
    }

    public void validateWatcher(boolean isWatching)
    {
        if ( isWatching && (watched == Allowance.CANNOT) )
        {
            throw new SchemaViolation(this, "Cannot be watched");
        }

        if ( !isWatching && (watched == Allowance.MUST) )
        {
            throw new SchemaViolation(this, "Must be watched");
        }
    }

    public void validateCreate(CreateMode mode, byte[] data)
    {
        if ( mode.isEphemeral() && (ephemeral == Allowance.CANNOT) )
        {
            throw new SchemaViolation(this, "Cannot be ephemeral");
        }

        if ( !mode.isEphemeral() && (ephemeral == Allowance.MUST) )
        {
            throw new SchemaViolation(this, "Must be ephemeral");
        }

        if ( mode.isSequential() && (sequential == Allowance.CANNOT) )
        {
            throw new SchemaViolation(this, "Cannot be sequential");
        }

        if ( !mode.isSequential() && (sequential == Allowance.MUST) )
        {
            throw new SchemaViolation(this, "Must be sequential");
        }

        validateData(data);
    }

    public void validateData(byte[] data)
    {
        if ( !dataValidator.isValid(data) )
        {
            throw new SchemaViolation(this, "Data is not valid");
        }
    }

    public Pattern getPath()
    {
        return path;
    }

    public String getDocumentation()
    {
        return documentation;
    }

    public DataValidator getDataValidator()
    {
        return dataValidator;
    }

    public Allowance getEphemeral()
    {
        return ephemeral;
    }

    public Allowance getSequential()
    {
        return sequential;
    }

    public Schema.Allowance getWatched()
    {
        return watched;
    }

    public boolean canBeDeleted()
    {
        return canBeDeleted;
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

        Schema schema = (Schema)o;

        return path.equals(schema.path);

    }

    @Override
    public int hashCode()
    {
        return path.hashCode();
    }

    @Override
    public String toString()
    {
        return "Schema{" +
            "path=" + path +
            ", documentation='" + documentation + '\'' +
            ", dataValidator=" + dataValidator +
            ", isEphemeral=" + ephemeral +
            ", isSequential=" + sequential +
            ", watched=" + watched +
            ", canBeDeleted=" + canBeDeleted +
            '}';
    }

    public String toDocumentation()
    {
        return path.pattern() + '\n'
            + documentation + '\n'
            + "Validator: " + dataValidator.getClass().getSimpleName() + '\n'
            + String.format("ephemeral: %s | sequential: %s | watched: %s | | canBeDeleted: %s", ephemeral, sequential, watched, canBeDeleted) + '\n'
            ;
    }
}
