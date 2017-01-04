package org.apache.curator.x.crimps;

import org.apache.zookeeper.KeeperException;

public class CrimpException extends RuntimeException
{
    private final KeeperException keeperException;

    public static KeeperException unwrap(Throwable e)
    {
        while ( e != null )
        {
            if ( e instanceof KeeperException )
            {
                return (KeeperException)e;
            }
            e = e.getCause();
        }
        return null;
    }

    public CrimpException(KeeperException keeperException)
    {
        super(keeperException);
        this.keeperException = keeperException;
    }

    public CrimpException(Throwable cause)
    {
        super(cause);
        keeperException = null;
    }

    public KeeperException getKeeperException()
    {
        return keeperException;
    }
}
