package org.apache.curator.x.crimps;

import org.apache.zookeeper.KeeperException;

class CrimpResult<T>
{
    final T value;
    final KeeperException exception;

    CrimpResult(T value)
    {
        this.value = value;
        this.exception = null;
    }

    CrimpResult(KeeperException exception)
    {
        this.value = null;
        this.exception = exception;
    }
}
