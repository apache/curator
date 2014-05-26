package org.apache.curator.x.rpc.idl;

import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;
import com.google.common.collect.Maps;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.Compressible;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.CreateModable;
import org.apache.curator.framework.api.PathAndBytesable;
import org.apache.curator.retry.RetryOneTime;
import java.util.Map;
import java.util.UUID;

@ThriftService("curator")
public class CuratorProjectionService
{
    private final Map<String, CuratorFramework> projections = Maps.newConcurrentMap();

    @ThriftMethod
    public CuratorProjection newCuratorProjection(CuratorProjectionSpec spec)
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient("localhost:2181", new RetryOneTime(1));
        String id = UUID.randomUUID().toString();
        client.start();
        projections.put(id, client);
        return new CuratorProjection(id);
    }

    @ThriftMethod
    public void closeCuratorProjection(CuratorProjection projection)
    {
        CuratorFramework client = projections.remove(projection.getId());
        if ( client != null )
        {
            client.close();
        }
    }

    //@ThriftMethod
    public String create(CuratorProjection projection, CreateSpec createSpec) throws Exception
    {
        CuratorFramework client = getClient(projection);

        Object builder = client.create();
        if ( createSpec.isCreatingParentsIfNeeded() )
        {
            builder = castBuilder(builder, CreateBuilder.class).creatingParentsIfNeeded();
        }
        if ( createSpec.isCompressed() )
        {
            builder = castBuilder(builder, Compressible.class).compressed();
        }
        if ( createSpec.isWithProtection() )
        {
            builder = castBuilder(builder, CreateBuilder.class).withProtection();
        }
        builder = castBuilder(builder, CreateModable.class).withMode(getRealMode(createSpec.getMode()));

        if ( createSpec.isAsync() )
        {
/*
TODO
            BackgroundCallback backgroundCallback = new RestBackgroundCallback(context, Constants.CLIENT_CREATE_ASYNC, createSpec.getAsyncId())
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    if ( event.getResultCode() == 0 )
                    {
                        checkEphemeralCreate(createSpec, id, event.getName());
                    }
                    super.processResult(client, event);
                }

                @Override
                protected String getMessage(CuratorEvent event)
                {
                    PathAndId pathAndId = new PathAndId(String.valueOf(event.getName()), id);
                    try
                    {
                        return context.getWriter().writeValueAsString(pathAndId);
                    }
                    catch ( IOException e )
                    {
                        log.error("Could not serialize PathAndId", e);
                    }
                    return "{}";
                }

                @Override
                protected String getDetails(CuratorEvent event)
                {
                    if ( event.getResultCode() != 0 )
                    {
                        return super.getDetails(event);
                    }
                    return id;
                }
            };
            builder = castBuilder(builder, Backgroundable.class).inBackground(backgroundCallback);
*/
        }

        return String.valueOf(castBuilder(builder, PathAndBytesable.class).forPath(createSpec.getPath(), createSpec.getData().getBytes()));
    }

    private org.apache.zookeeper.CreateMode getRealMode(CreateMode mode)
    {
        switch ( mode )
        {
            case PERSISTENT:
            {
                return org.apache.zookeeper.CreateMode.PERSISTENT;
            }

            case PERSISTENT_SEQUENTIAL:
            {
                return org.apache.zookeeper.CreateMode.PERSISTENT_SEQUENTIAL;
            }

            case EPHEMERAL:
            {
                return org.apache.zookeeper.CreateMode.EPHEMERAL;
            }

            case EPHEMERAL_SEQUENTIAL:
            {
                return org.apache.zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL;
            }
        }
        throw new UnsupportedOperationException("Bad mode: " + mode.toString());
    }

    private CuratorFramework getClient(CuratorProjection projection) throws Exception
    {
        CuratorFramework client = projections.get(projection.getId());
        if ( client == null )
        {
            throw new Exception("No client found with id: " + projection.getId());
        }
        return client;
    }

    private static <T> T castBuilder(Object createBuilder, Class<T> clazz) throws Exception
    {
        if ( clazz.isAssignableFrom(createBuilder.getClass()) )
        {
            return clazz.cast(createBuilder);
        }
        throw new Exception("That operation is not available"); // TODO
    }
}
