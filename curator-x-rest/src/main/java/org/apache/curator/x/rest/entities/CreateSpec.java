package org.apache.curator.x.rest.entities;

import org.apache.zookeeper.CreateMode;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
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

    public CreateSpec()
    {
        this("/", "", CreateMode.PERSISTENT, false, "", false, false, false);
    }

    public CreateSpec(String path, String data, CreateMode mode, boolean async, String asyncId, boolean compressed, boolean creatingParentsIfNeeded, boolean withProtection)
    {
        this.path = path;
        this.data = data;
        this.mode = mode;
        this.async = async;
        this.asyncId = asyncId;
        this.compressed = compressed;
        this.creatingParentsIfNeeded = creatingParentsIfNeeded;
        this.withProtection = withProtection;
    }

    public String getPath()
    {
        return path;
    }

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

    public String getData()
    {
        return data;
    }

    public void setData(String data)
    {
        this.data = data;
    }

    public CreateMode getMode()
    {
        return mode;
    }

    public void setMode(CreateMode mode)
    {
        this.mode = mode;
    }

    public boolean isAsync()
    {
        return async;
    }

    public void setAsync(boolean async)
    {
        this.async = async;
    }

    public boolean isCompressed()
    {
        return compressed;
    }

    public void setCompressed(boolean compressed)
    {
        this.compressed = compressed;
    }

    public boolean isCreatingParentsIfNeeded()
    {
        return creatingParentsIfNeeded;
    }

    public void setCreatingParentsIfNeeded(boolean creatingParentsIfNeeded)
    {
        this.creatingParentsIfNeeded = creatingParentsIfNeeded;
    }

    public boolean isWithProtection()
    {
        return withProtection;
    }

    public void setWithProtection(boolean withProtection)
    {
        this.withProtection = withProtection;
    }
}
