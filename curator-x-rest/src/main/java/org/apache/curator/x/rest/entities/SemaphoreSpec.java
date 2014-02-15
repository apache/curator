package org.apache.curator.x.rest.entities;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class SemaphoreSpec
{
    private String path;
    private int acquireQty;
    private int maxWaitMs;
    private int maxLeases;

    public SemaphoreSpec()
    {
        this("", 0, 0, 0);
    }

    public SemaphoreSpec(String path, int acquireQty, int maxWaitMs, int maxLeases)
    {
        this.path = path;
        this.acquireQty = acquireQty;
        this.maxWaitMs = maxWaitMs;
        this.maxLeases = maxLeases;
    }

    public int getAcquireQty()
    {
        return acquireQty;
    }

    public void setAcquireQty(int acquireQty)
    {
        this.acquireQty = acquireQty;
    }

    public String getPath()
    {
        return path;
    }

    public void setPath(String path)
    {
        this.path = path;
    }

    public int getMaxWaitMs()
    {
        return maxWaitMs;
    }

    public void setMaxWaitMs(int maxWaitMs)
    {
        this.maxWaitMs = maxWaitMs;
    }

    public int getMaxLeases()
    {
        return maxLeases;
    }

    public void setMaxLeases(int maxLeases)
    {
        this.maxLeases = maxLeases;
    }
}
