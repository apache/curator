package pubsub.models;

import org.apache.curator.x.async.modeled.NodeName;

public class Group implements NodeName
{
    private final String groupName;

    public Group()
    {
        this("");
    }

    public Group(String groupName)
    {
        this.groupName = groupName;
    }

    public String getGroupName()
    {
        return groupName;
    }

    @Override
    public String nodeName()
    {
        return groupName;
    }
}
