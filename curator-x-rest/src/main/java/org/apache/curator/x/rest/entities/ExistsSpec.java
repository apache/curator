package org.apache.curator.x.rest.entities;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ExistsSpec
{
    private String path;
    private boolean watched;
    private String watchId;
    private boolean async;
    private String asyncId;

    public ExistsSpec()
    {
        this("/", false, "", false, "");
    }

    public ExistsSpec(String path, boolean watched, String watchId, boolean async, String asyncId)
    {
        this.path = path;
        this.watched = watched;
        this.watchId = watchId;
        this.async = async;
        this.asyncId = asyncId;
    }

    public String getWatchId()
    {
        return watchId;
    }

    public void setWatchId(String watchId)
    {
        this.watchId = watchId;
    }

    public String getPath()
    {
        return path;
    }

    public void setPath(String path)
    {
        this.path = path;
    }

    public boolean isWatched()
    {
        return watched;
    }

    public void setWatched(boolean watched)
    {
        this.watched = watched;
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
}
