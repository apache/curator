package org.apache.curator.x.rest.entities;

public class LeaderSpec
{
    private String path;
    private String id;

    public LeaderSpec()
    {
        this("/", "");
    }

    public LeaderSpec(String path, String id)
    {
        this.path = path;
        this.id = id;
    }

    public String getPath()
    {
        return path;
    }

    public void setPath(String path)
    {
        this.path = path;
    }

    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }
}
