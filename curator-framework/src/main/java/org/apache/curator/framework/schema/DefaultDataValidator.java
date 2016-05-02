package org.apache.curator.framework.schema;

public class DefaultDataValidator implements DataValidator
{
    @Override
    public boolean isValid(byte[] data)
    {
        return true;
    }
}
