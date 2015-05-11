package org.apache.curator.test;

import org.apache.zookeeper.ZooKeeper;
import java.lang.reflect.Method;
import java.util.List;

public class WatchersDebug
{
    private static final Method getDataWatches;
    private static final Method getExistWatches;
    private static final Method getChildWatches;
    static
    {
        Method localGetDataWatches = null;
        Method localGetExistWatches = null;
        Method localGetChildWatches = null;
        try
        {
            localGetDataWatches = getMethod("getDataWatches");
            localGetExistWatches = getMethod("getExistWatches");
            localGetChildWatches = getMethod("getChildWatches");
        }
        catch ( NoSuchMethodException e )
        {
            e.printStackTrace();
        }
        getDataWatches = localGetDataWatches;
        getExistWatches = localGetExistWatches;
        getChildWatches = localGetChildWatches;
    }

    public static List<String> getDataWatches(ZooKeeper zooKeeper)
    {
        return callMethod(zooKeeper, WatchersDebug.getDataWatches);
    }

    public static List<String> getExistWatches(ZooKeeper zooKeeper)
    {
        return callMethod(zooKeeper, getExistWatches);
    }

    public static List<String> getChildWatches(ZooKeeper zooKeeper)
    {
        return callMethod(zooKeeper, getChildWatches);
    }

    private WatchersDebug()
    {
    }

    private static Method getMethod(String name) throws NoSuchMethodException
    {
        Method m = ZooKeeper.class.getDeclaredMethod(name);
        m.setAccessible(true);
        return m;
    }

    private static List<String> callMethod(ZooKeeper zooKeeper, Method method)
    {
        if ( zooKeeper == null )
        {
            return null;
        }
        try
        {
            //noinspection unchecked
            return (List<String>)method.invoke(zooKeeper);
        }
        catch ( Exception e )
        {
            throw new RuntimeException(e);
        }
    }
}
