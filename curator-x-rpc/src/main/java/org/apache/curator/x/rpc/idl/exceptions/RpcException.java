/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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
