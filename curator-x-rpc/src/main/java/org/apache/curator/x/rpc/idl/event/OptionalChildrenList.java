package org.apache.curator.x.rpc.idl.event;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import java.util.List;

@ThriftStruct
public class OptionalChildrenList
{
    @ThriftField(1)
    public List<String> children;

    public OptionalChildrenList()
    {
    }

    public OptionalChildrenList(List<String> children)
    {
        this.children = children;
    }
}
