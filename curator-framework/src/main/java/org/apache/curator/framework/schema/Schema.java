package org.apache.curator.framework.schema;

import com.google.common.base.Preconditions;
import org.apache.zookeeper.CreateMode;
import java.util.regex.Pattern;

/**
 * Represents and documents operations allowed for a given path pattern
 */
public class Schema
{
    private final Pattern pathRegex;
    private final String path;
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
     * Start a builder for the given path pattern. Note: full path schemas
     * take precedence over regex path schemas.
     *
     * @param path full ZNode path. This schema only applies to an exact match
     * @return builder
     */
    public static SchemaBuilder builder(String path)
    {
        return new SchemaBuilder(null, path);
    }

    /**
     * Start a builder for the given path pattern.
     *
     * @param pathRegex regex for the path. This schema applies to any matching paths
     * @return builder
     */
    public static SchemaBuilder builder(Pattern pathRegex)
    {
        return new SchemaBuilder(pathRegex, null);
    }

    Schema(Pattern pathRegex, String path, String documentation, DataValidator dataValidator, Allowance ephemeral, Allowance sequential, Allowance watched, boolean canBeDeleted)
    {
        Preconditions.checkNotNull((pathRegex != null) || (path != null), "pathRegex and path cannot both be null");
        this.pathRegex = pathRegex;
        this.path = path;
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

    public Pattern getPathRegex()
    {
        return pathRegex;
    }

    public String getPath()
    {
        return path;
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

        return pathRegex.equals(schema.pathRegex);

    }

    @Override
    public int hashCode()
    {
        return pathRegex.hashCode();
    }

    @Override
    public String toString()
    {
        return "Schema{" +
            "path=" + pathRegex +
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
        return pathRegex.pattern() + '\n'
            + documentation + '\n'
            + "Validator: " + dataValidator.getClass().getSimpleName() + '\n'
            + String.format("ephemeral: %s | sequential: %s | watched: %s | | canBeDeleted: %s", ephemeral, sequential, watched, canBeDeleted) + '\n'
            ;
    }
}
