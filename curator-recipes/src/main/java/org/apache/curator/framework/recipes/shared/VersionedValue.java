package org.apache.curator.framework.recipes.shared;

import com.google.common.base.Preconditions;

/**
 * POJO for a version and a value
 */
public class VersionedValue<T>
{
    private final int version;
    private final T value;

    /**
     * @param version the version
     * @param value the value (cannot be null)
     */
    public VersionedValue(int version, T value)
    {
        this.version = version;
        this.value = Preconditions.checkNotNull(value, "value cannot be null");
    }

    public int getVersion()
    {
        return version;
    }

    public T getValue()
    {
        return value;
    }
}
