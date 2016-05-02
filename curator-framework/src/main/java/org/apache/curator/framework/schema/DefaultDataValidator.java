package org.apache.curator.framework.schema;

/**
 * The default data validator - always returns true
 */
public class DefaultDataValidator implements DataValidator
{
    @Override
    public boolean isValid(byte[] data)
    {
        return true;
    }
}
