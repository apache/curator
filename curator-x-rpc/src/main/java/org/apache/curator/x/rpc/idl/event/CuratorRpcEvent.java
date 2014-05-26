package org.apache.curator.x.rpc.idl.event;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.x.rpc.idl.projection.CuratorProjection;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import javax.annotation.Nullable;
import java.util.List;

@SuppressWarnings("deprecation")
@ThriftStruct("CuratorEvent")
public class CuratorRpcEvent
{
    private final CuratorProjection projection;
    private final CuratorEvent event;

    public CuratorRpcEvent()
    {
        throw new UnsupportedOperationException();
    }

    public CuratorRpcEvent(CuratorProjection projection, CuratorEvent event)
    {
        this.projection = projection;
        this.event = event;
    }

    @ThriftField(1)
    public CuratorProjection getProjection()
    {
        return projection;
    }

    @ThriftField(2)
    public CuratorRpcEventType getType()
    {
        switch ( event.getType() )
        {
            case CREATE:
            {
                return CuratorRpcEventType.CREATE;
            }

            case DELETE:
            {
                return CuratorRpcEventType.DELETE;
            }

            case EXISTS:
            {
                return CuratorRpcEventType.EXISTS;
            }

            case GET_DATA:
            {
                return CuratorRpcEventType.GET_DATA;
            }

            case SET_DATA:
            {
                return CuratorRpcEventType.SET_DATA;
            }

            case CHILDREN:
            {
                return CuratorRpcEventType.CHILDREN;
            }

            case SYNC:
            {
                return CuratorRpcEventType.SYNC;
            }

            case GET_ACL:
            {
                return CuratorRpcEventType.GET_ACL;
            }

            case SET_ACL:
            {
                return CuratorRpcEventType.SET_ACL;
            }

            case WATCHED:
            {
                return CuratorRpcEventType.WATCHED;
            }

            case CLOSING:
            {
                return CuratorRpcEventType.CLOSING;
            }
        }

        throw new IllegalStateException("Unknown type: " + event.getType());
    }

    @ThriftField(3)
    public int getResultCode()
    {
        return event.getResultCode();
    }

    @ThriftField(4)
    public String getPath()
    {
        return event.getPath();
    }

    @ThriftField(5)
    public String getContext()
    {
        return String.valueOf(event.getContext());
    }

    @ThriftField(6)
    public RpcStat getStat()
    {
        Stat stat = event.getStat();
        if ( stat != null )
        {
            return new RpcStat
            (
                stat.getCzxid(),
                stat.getMzxid(),
                stat.getCtime(),
                stat.getMtime(),
                stat.getVersion(),
                stat.getCversion(),
                stat.getAversion(),
                stat.getEphemeralOwner(),
                stat.getDataLength(),
                stat.getNumChildren(),
                stat.getPzxid()
            );
        }
        return null;
    }

    @ThriftField(7)
    public byte[] getData()
    {
        return event.getData();
    }

    @ThriftField(8)
    public String getName()
    {
        return event.getPath();
    }

    @ThriftField(9)
    public List<String> getChildren()
    {
        return event.getChildren();
    }

    @ThriftField(10)
    public List<RpcAcl> getACLList()
    {
        List<ACL> aclList = event.getACLList();
        if ( aclList != null )
        {
            return Lists.transform
            (
                aclList,
                new Function<ACL, RpcAcl>()
                {
                    @Nullable
                    @Override
                    public RpcAcl apply(ACL acl)
                    {
                        RpcId id = new RpcId(acl.getId().getScheme(), acl.getId().getId());
                        return new RpcAcl(acl.getPerms(), id);
                    }
                }
            );
        }
        return null;
    }

    @ThriftField(11)
    public RpcWatchedEvent getWatchedEvent()
    {
        WatchedEvent watchedEvent = event.getWatchedEvent();
        if ( watchedEvent != null )
        {
            RpcKeeperState keeperState = toRpcKeeperState(watchedEvent.getState());
            RpcEventType eventType = toRpcEventType(watchedEvent.getType());
            return new RpcWatchedEvent(keeperState, eventType, watchedEvent.getPath());
        }
        return null;
    }

    private RpcEventType toRpcEventType(Watcher.Event.EventType type)
    {
        switch ( type )
        {
            case None:
            {
                return RpcEventType.None;
            }

            case NodeCreated:
            {
                return RpcEventType.NodeCreated;
            }

            case NodeDeleted:
            {
                return RpcEventType.NodeDeleted;
            }

            case NodeDataChanged:
            {
                return RpcEventType.NodeDataChanged;
            }

            case NodeChildrenChanged:
            {
                return RpcEventType.NodeChildrenChanged;
            }
        }
        throw new IllegalStateException("Unknown type: " + type);
    }

    private RpcKeeperState toRpcKeeperState(Watcher.Event.KeeperState state)
    {
        switch ( state )
        {
            case Unknown:
            {
                return RpcKeeperState.Unknown;
            }

            case Disconnected:
            {
                return RpcKeeperState.Disconnected;
            }

            case NoSyncConnected:
            {
                return RpcKeeperState.NoSyncConnected;
            }

            case SyncConnected:
            {
                return RpcKeeperState.SyncConnected;
            }

            case AuthFailed:
            {
                return RpcKeeperState.AuthFailed;
            }

            case ConnectedReadOnly:
            {
                return RpcKeeperState.ConnectedReadOnly;
            }

            case SaslAuthenticated:
            {
                return RpcKeeperState.SaslAuthenticated;
            }

            case Expired:
            {
                return RpcKeeperState.Expired;
            }
        }
        throw new IllegalStateException("Unknown state: " + state);
    }
}
