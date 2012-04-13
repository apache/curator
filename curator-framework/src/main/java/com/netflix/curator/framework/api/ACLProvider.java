package com.netflix.curator.framework.api;

import org.apache.zookeeper.data.ACL;
import java.util.List;

public interface ACLProvider
{
    public List<ACL>        getDefaultAcl();

    public List<ACL>        getAclForPath(String path);
}
