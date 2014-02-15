package org.apache.curator.x.rest.entities;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class DeleteSpec
{
    private String path;
    private boolean async;
    private String asyncId;
    private boolean guaranteed;
    private int version;

    public DeleteSpec()
    {
        this("/", false, "", false, -1);
    }

    public DeleteSpec(String path, boolean async, String asyncId, boolean guaranteed, int version)
    {
        this.path = path;
        this.async = async;
        this.asyncId = asyncId;
        this.guaranteed = guaranteed;
        this.version = version;
    }

    public String getPath()
    {
        return path;
    }

    public void setPath(String path)
    {
        this.path = path;
    }

    public boolean isAsync()
    {
        return async;
    }

    public void setAsync(boolean async)
    {
        this.async = async;
    }

    public String getAsyncId()
    {
        return asyncId;
    }

    public void setAsyncId(String asyncId)
    {
        this.asyncId = asyncId;
    }

    public boolean isGuaranteed()
    {
        return guaranteed;
    }

    public void setGuaranteed(boolean guaranteed)
    {
        this.guaranteed = guaranteed;
    }

    public int getVersion()
    {
        return version;
    }

    public void setVersion(int version)
    {
        this.version = version;
    }
}
