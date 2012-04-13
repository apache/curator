package com.netflix.curator.framework.api;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import java.util.List;

public interface ACLProvider
{
    /**
     * Return the ACL list to use by default (usually {@link ZooDefs.Ids#OPEN_ACL_UNSAFE}).
     *
     * @return default ACL list
     */
    public List<ACL>        getDefaultAcl();

    /**
     * Return the ACL list to use for the given path
     *
     * @param path path (NOTE: might be null)
     * @return ACL list
     */
    public List<ACL>        getAclForPath(String path);
}
