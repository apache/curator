package org.apache.curator.x.rpc.idl.projection;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct
public class CreateSpec
{
    private String path;
    private String data;
    private CreateMode mode;
    private boolean async;
    private String asyncId;
    private boolean compressed;
    private boolean creatingParentsIfNeeded;
    private boolean withProtection;

    @ThriftField(1)
    public String getPath()
    {
        return path;
    }

    @ThriftField(2)
    public String getAsyncId()
    {
        return asyncId;
    }

    public void setAsyncId(String asyncId)
    {
        this.asyncId = asyncId;
    }

    public void setPath(String path)
    {
        this.path = path;
    }

    @ThriftField(3)
    public String getData()
    {
        return data;
    }

    public void setData(String data)
    {
        this.data = data;
    }

    @ThriftField(4)
    public CreateMode getMode()
    {
        return mode;
    }

    public void setMode(CreateMode mode)
    {
        this.mode = mode;
    }

    @ThriftField(5)
    public boolean isAsync()
    {
        return async;
    }

    public void setAsync(boolean async)
    {
        this.async = async;
    }

    @ThriftField(6)
    public boolean isCompressed()
    {
        return compressed;
    }

    public void setCompressed(boolean compressed)
    {
        this.compressed = compressed;
    }

    @ThriftField(7)
    public boolean isCreatingParentsIfNeeded()
    {
        return creatingParentsIfNeeded;
    }

    public void setCreatingParentsIfNeeded(boolean creatingParentsIfNeeded)
    {
        this.creatingParentsIfNeeded = creatingParentsIfNeeded;
    }

    @ThriftField(8)
    public boolean isWithProtection()
    {
        return withProtection;
    }

    public void setWithProtection(boolean withProtection)
    {
        this.withProtection = withProtection;
    }
}
