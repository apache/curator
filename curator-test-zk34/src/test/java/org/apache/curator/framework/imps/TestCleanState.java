package org.apache.curator.framework.imps;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.CloseableUtils;
import java.util.concurrent.Callable;

public class TestCleanState
{
    public static void closeAndTestClean(CuratorFramework client)
    {
        CloseableUtils.closeQuietly(client);
    }

    public static void test(CuratorFramework client, Callable<Void> proc) throws Exception
    {
        try
        {
            proc.call();
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }
}
