package org.apache.curator.x.rest.entities;

import org.apache.zookeeper.data.Stat;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class NodeData
{
    private String path;
    private Stat stat;
    private String data;

    public NodeData()
    {
        this("/", new Stat(), "");
    }

    public NodeData(String path, Stat stat, String data)
    {
        this.path = path;
        this.stat = stat;
        this.data = data;
    }

    public String getPath()
    {
        return path;
    }

    public void setPath(String path)
    {
        this.path = path;
    }

    public Stat getStat()
    {
        return stat;
    }

    public void setStat(Stat stat)
    {
        this.stat = stat;
    }

    public String getData()
    {
        return data;
    }

    public void setData(String data)
    {
        this.data = data;
    }
}
