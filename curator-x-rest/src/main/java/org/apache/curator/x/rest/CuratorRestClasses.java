package org.apache.curator.x.rest;

import com.google.common.collect.ImmutableList;
import org.apache.curator.x.rest.api.ClientResource;
import org.apache.curator.x.rest.api.LeaderResource;
import org.apache.curator.x.rest.api.LockResource;
import org.apache.curator.x.rest.api.NodeCacheResource;
import org.apache.curator.x.rest.api.PathChildrenCacheResource;
import org.apache.curator.x.rest.api.PersistentEphemeralNodeResource;
import org.apache.curator.x.rest.api.ReadWriteLockResource;
import org.apache.curator.x.rest.api.SemaphoreResource;
import org.apache.curator.x.rest.api.SessionResource;
import java.util.List;

public class CuratorRestClasses
{
    public static List<Class<?>> getClasses()
    {
        ImmutableList.Builder<Class<?>> builder = ImmutableList.builder();
        builder.add(SessionResource.class);
        builder.add(ClientResource.class);
        builder.add(LockResource.class);
        builder.add(SemaphoreResource.class);
        builder.add(PathChildrenCacheResource.class);
        builder.add(NodeCacheResource.class);
        builder.add(LeaderResource.class);
        builder.add(ReadWriteLockResource.class);
        builder.add(PersistentEphemeralNodeResource.class);
        return builder.build();
    }

    private CuratorRestClasses()
    {
    }
}
