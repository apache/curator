package com.netflix.curator.framework.imps;

import com.netflix.curator.framework.api.ACLProvider;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import java.util.List;

public class DefaultACLProvider implements ACLProvider
{
    @Override
    public List<ACL> getDefaultAcl()
    {
        return ZooDefs.Ids.OPEN_ACL_UNSAFE;
    }

    @Override
    public List<ACL> getAclForPath(String path)
    {
        return ZooDefs.Ids.OPEN_ACL_UNSAFE;
    }
}
