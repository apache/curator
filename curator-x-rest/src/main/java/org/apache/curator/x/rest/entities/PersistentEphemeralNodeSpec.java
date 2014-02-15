package org.apache.curator.x.rest.entities;

import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;

public class PersistentEphemeralNodeSpec
{
    private String path;
    private String data;
    private PersistentEphemeralNode.Mode mode;

    public PersistentEphemeralNodeSpec()
    {
        this("/", "", PersistentEphemeralNode.Mode.EPHEMERAL);
    }

    public PersistentEphemeralNodeSpec(String path, String data, PersistentEphemeralNode.Mode mode)
    {
        this.path = path;
        this.data = data;
        this.mode = mode;
    }

    public String getPath()
    {
        return path;
    }

    public void setPath(String path)
    {
        this.path = path;
    }

    public String getData()
    {
        return data;
    }

    public void setData(String data)
    {
        this.data = data;
    }

    public PersistentEphemeralNode.Mode getMode()
    {
        return mode;
    }

    public void setMode(PersistentEphemeralNode.Mode mode)
    {
        this.mode = mode;
    }
}
