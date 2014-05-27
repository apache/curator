package org.apache.curator.x.rpc;

import com.facebook.swift.service.ThriftServerConfig;
import io.airlift.configuration.Config;

public class Configuration extends ThriftServerConfig
{
    @Config("hey")
    public void setHey(int hey)
    {

    }
}
