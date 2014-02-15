package org.apache.curator.x.rest.entities;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class SetDataSpec
{
    private String path;
    private boolean watched;
    private String watchId;
    private boolean async;
    private String asyncId;
    private boolean compressed;
    private int version;
    private String data;

    public SetDataSpec()
    {
        this("/", false, "", false, "", false, -1, "");
    }

    public SetDataSpec(String path, boolean watched, String watchId, boolean async, String asyncId, boolean compressed, int version, String data)
    {
        this.path = path;
        this.watched = watched;
        this.watchId = watchId;
        this.async = async;
        this.asyncId = asyncId;
        this.compressed = compressed;
        this.version = version;
        this.data = data;
    }

    public String getData()
    {
        return data;
    }

    public void setData(String data)
    {
        this.data = data;
    }

    public int getVersion()
    {
        return version;
    }

    public void setVersion(int version)
    {
        this.version = version;
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

    public boolean isCompressed()
    {
        return compressed;
    }

    public void setCompressed(boolean compressed)
    {
        this.compressed = compressed;
    }
}
