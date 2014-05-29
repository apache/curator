package org.apache.curator.x.rpc;

import com.google.common.io.Resources;
import org.apache.curator.test.TestingServer;
import java.nio.charset.Charset;

public class TestServer
{
    public static void main(String[] args) throws Exception
    {
        new TestingServer(2181);

        String configurationSource = Resources.toString(Resources.getResource("configuration/test.json"), Charset.defaultCharset());
        CuratorProjectionServer.main(new String[]{configurationSource});
    }
}
