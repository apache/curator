package org.apache.curator.framework.schema;

public interface DataValidator
{
    boolean isValid(byte[] data);
}
