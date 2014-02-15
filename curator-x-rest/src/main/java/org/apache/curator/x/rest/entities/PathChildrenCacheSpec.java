package org.apache.curator.x.rest.entities;

import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class PathChildrenCacheSpec
{
    private String path;
    private boolean cacheData;
    private boolean dataIsCompressed;
    private PathChildrenCache.StartMode startMode;

    public PathChildrenCacheSpec()
    {
        this("", false, false, PathChildrenCache.StartMode.NORMAL);
    }

    public PathChildrenCacheSpec(String path, boolean cacheData, boolean dataIsCompressed, PathChildrenCache.StartMode startMode)
    {
        this.path = path;
        this.cacheData = cacheData;
        this.dataIsCompressed = dataIsCompressed;
        this.startMode = startMode;
    }

    public String getPath()
    {
        return path;
    }

    public void setPath(String path)
    {
        this.path = path;
    }

    public boolean isCacheData()
    {
        return cacheData;
    }

    public void setCacheData(boolean cacheData)
    {
        this.cacheData = cacheData;
    }

    public boolean isDataIsCompressed()
    {
        return dataIsCompressed;
    }

    public void setDataIsCompressed(boolean dataIsCompressed)
    {
        this.dataIsCompressed = dataIsCompressed;
    }

    public PathChildrenCache.StartMode getStartMode()
    {
        return startMode;
    }

    public void setStartMode(PathChildrenCache.StartMode startMode)
    {
        this.startMode = startMode;
    }
}
