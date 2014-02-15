package org.apache.curator.x.rest.entities;

import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class NodeCacheSpec
{
    private String path;
    private boolean dataIsCompressed;
    private boolean buildInitial;

    public NodeCacheSpec()
    {
        this("/", false, false);
    }

    public NodeCacheSpec(String path, boolean dataIsCompressed, boolean buildInitial)
    {
        this.path = path;
        this.dataIsCompressed = dataIsCompressed;
        this.buildInitial = buildInitial;
    }

    public String getPath()
    {
        return path;
    }

    public void setPath(String path)
    {
        this.path = path;
    }

    public boolean isDataIsCompressed()
    {
        return dataIsCompressed;
    }

    public void setDataIsCompressed(boolean dataIsCompressed)
    {
        this.dataIsCompressed = dataIsCompressed;
    }

    public boolean isBuildInitial()
    {
        return buildInitial;
    }

    public void setBuildInitial(boolean buildInitial)
    {
        this.buildInitial = buildInitial;
    }
}
