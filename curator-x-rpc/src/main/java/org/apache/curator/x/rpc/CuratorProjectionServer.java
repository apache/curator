package org.apache.curator.x.rpc;

import com.facebook.swift.codec.ThriftCodecManager;
import com.facebook.swift.service.ThriftEventHandler;
import com.facebook.swift.service.ThriftServer;
import com.facebook.swift.service.ThriftServerConfig;
import com.facebook.swift.service.ThriftServiceProcessor;
import com.google.common.collect.Lists;
import org.apache.curator.x.rpc.idl.CuratorProjectionService;

public class CuratorProjectionServer
{
    public static void main(String[] args)
    {
        CuratorProjectionService projectionService = new CuratorProjectionService();
        ThriftServiceProcessor processor = new ThriftServiceProcessor(new ThriftCodecManager(), Lists.<ThriftEventHandler>newArrayList(), projectionService);
        ThriftServer server = new ThriftServer(processor, new ThriftServerConfig().setPort(8899));  // TODO
        server.start();
    }
}
