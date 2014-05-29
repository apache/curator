package org.apache.curator.x.rpc;

import com.google.common.io.Resources;
import java.nio.charset.Charset;

public class TestServer
{
    public static void main(String[] args) throws Exception
    {
        String configurationSource = Resources.toString(Resources.getResource("configuration/test.json"), Charset.defaultCharset());
        CuratorProjectionServer.main(new String[]{configurationSource});
    }
}
