package org.apache.curator.x.rpc.idl.exceptions;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.facebook.swift.service.ThriftException;
import org.apache.zookeeper.KeeperException;
import java.io.PrintWriter;
import java.io.StringWriter;

@ThriftException(id = 1, type = RpcException.class, name = "CuratorException")
@ThriftStruct("CuratorException")
public class RpcException extends Exception
{
    @ThriftField(1)
    public ExceptionType type;

    @ThriftField(2)
    public ZooKeeperExceptionType zooKeeperException;

    @ThriftField(3)
    public NodeExceptionType nodeException;

    @ThriftField(4)
    public String message;

    public RpcException()
    {
    }

    public RpcException(Exception e)
    {
        this.message = e.getLocalizedMessage();
        if ( this.message == null )
        {
            StringWriter str = new StringWriter();
            e.printStackTrace(new PrintWriter(str));
            this.message = str.toString();
        }

        if ( KeeperException.class.isAssignableFrom(e.getClass()) )
        {
            KeeperException keeperException = (KeeperException)e;
            switch ( keeperException.code() )
            {
                default:
                {
                    type = ExceptionType.ZOOKEEPER;
                    zooKeeperException = ZooKeeperExceptionType.valueOf(keeperException.code().name());
                    nodeException = null;
                    break;
                }

                case NONODE:
                case NODEEXISTS:
                case NOTEMPTY:
                case BADVERSION:
                {
                    type = ExceptionType.NODE;
                    zooKeeperException = null;
                    nodeException = NodeExceptionType.valueOf(keeperException.code().name());
                    break;
                }
            }
        }
        else
        {
            type = ExceptionType.GENERAL;
        }
    }

    public RpcException(ExceptionType type, ZooKeeperExceptionType zooKeeperException, NodeExceptionType nodeException, String message)
    {
        this.type = type;
        this.zooKeeperException = zooKeeperException;
        this.nodeException = nodeException;
        this.message = message;
    }


}
