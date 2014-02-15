package org.apache.curator.x.rest.entities;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class LockSpec
{
    private String path;
    private int maxWaitMs;

    public LockSpec()
    {
        this("", 0);
    }

    public LockSpec(String path, int maxWaitMs)
    {
        this.path = path;
        this.maxWaitMs = maxWaitMs;
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
}
