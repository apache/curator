package org.apache.curator.x.rest.entities;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class GetChildrenSpec
{
    private String path;
    private boolean async;
    private String asyncId;
    private String asyncListSeparator;
    private boolean watched;
    private String watchId;

    public GetChildrenSpec()
    {
        this("/", false, "", ",", false, "");
    }

    public GetChildrenSpec(String path, boolean async, String asyncId, String asyncListSeparator, boolean watched, String watchId)
    {
        this.path = path;
        this.async = async;
        this.asyncId = asyncId;
        this.asyncListSeparator = asyncListSeparator;
        this.watched = watched;
        this.watchId = watchId;
    }

    public String getAsyncListSeparator()
    {
        return asyncListSeparator;
    }

    public void setAsyncListSeparator(String asyncListSeparator)
    {
        this.asyncListSeparator = asyncListSeparator;
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

    public boolean isWatched()
    {
        return watched;
    }

    public void setWatched(boolean watched)
    {
        this.watched = watched;
    }

    public String getWatchId()
    {
        return watchId;
    }

    public void setWatchId(String watchId)
    {
        this.watchId = watchId;
    }
}
