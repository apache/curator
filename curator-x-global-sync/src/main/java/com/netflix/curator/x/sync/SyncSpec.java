package com.netflix.curator.x.sync;

public class SyncSpec
{
    private final String path;
    private final SyncTypes type;

    public SyncSpec(String path, SyncTypes type)
    {
        this.path = path;
        this.type = type;
    }

    public String getPath()
    {
        return path;
    }

    public SyncTypes getType()
    {
        return type;
    }
}
