package org.apache.curator.x.rpc.idl.event;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct("Stat")
public class RpcStat
{
    private long czxid;
    private long mzxid;
    private long ctime;
    private long mtime;
    private int version;
    private int cversion;
    private int aversion;
    private long ephemeralOwner;
    private int dataLength;
    private int numChildren;
    private long pzxid;

    public RpcStat()
    {
    }

    public RpcStat(long czxid, long mzxid, long ctime, long mtime, int version, int cversion, int aversion, long ephemeralOwner, int dataLength, int numChildren, long pzxid)
    {
        this.czxid = czxid;
        this.mzxid = mzxid;
        this.ctime = ctime;
        this.mtime = mtime;
        this.version = version;
        this.cversion = cversion;
        this.aversion = aversion;
        this.ephemeralOwner = ephemeralOwner;
        this.dataLength = dataLength;
        this.numChildren = numChildren;
        this.pzxid = pzxid;
    }

    @ThriftField(1)
    public long getCzxid()
    {
        return czxid;
    }

    public void setCzxid(long czxid)
    {
        this.czxid = czxid;
    }

    @ThriftField(2)
    public long getMzxid()
    {
        return mzxid;
    }

    public void setMzxid(long mzxid)
    {
        this.mzxid = mzxid;
    }

    @ThriftField(3)
    public long getCtime()
    {
        return ctime;
    }

    public void setCtime(long ctime)
    {
        this.ctime = ctime;
    }

    @ThriftField(4)
    public long getMtime()
    {
        return mtime;
    }

    public void setMtime(long mtime)
    {
        this.mtime = mtime;
    }

    @ThriftField(5)
    public int getVersion()
    {
        return version;
    }

    public void setVersion(int version)
    {
        this.version = version;
    }

    @ThriftField(6)
    public int getCversion()
    {
        return cversion;
    }

    public void setCversion(int cversion)
    {
        this.cversion = cversion;
    }

    @ThriftField(7)
    public int getAversion()
    {
        return aversion;
    }

    public void setAversion(int aversion)
    {
        this.aversion = aversion;
    }

    @ThriftField(8)
    public long getEphemeralOwner()
    {
        return ephemeralOwner;
    }

    public void setEphemeralOwner(long ephemeralOwner)
    {
        this.ephemeralOwner = ephemeralOwner;
    }

    @ThriftField(9)
    public int getDataLength()
    {
        return dataLength;
    }

    public void setDataLength(int dataLength)
    {
        this.dataLength = dataLength;
    }

    @ThriftField(10)
    public int getNumChildren()
    {
        return numChildren;
    }

    public void setNumChildren(int numChildren)
    {
        this.numChildren = numChildren;
    }

    @ThriftField(11)
    public long getPzxid()
    {
        return pzxid;
    }

    public void setPzxid(long pzxid)
    {
        this.pzxid = pzxid;
    }
}
