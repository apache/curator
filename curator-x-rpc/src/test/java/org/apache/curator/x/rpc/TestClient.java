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
package org.apache.curator.x.rpc;

import org.apache.curator.generated.CuratorEvent;
import org.apache.curator.generated.CuratorProjection;
import org.apache.curator.generated.CuratorProjectionSpec;
import org.apache.curator.generated.CuratorService;
import org.apache.curator.generated.EventService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import java.util.concurrent.Executors;

public class TestClient
{
    public static void main(String[] args) throws TException
    {
        TSocket clientTransport = new TSocket("localhost", 8899);
        clientTransport.open();
        TProtocol clientProtocol = new TBinaryProtocol(clientTransport);
        final CuratorService.Client client = new CuratorService.Client(clientProtocol);

        TSocket eventTransport = new TSocket("localhost", 8899);
        eventTransport.open();
        TProtocol eventProtocol = new TBinaryProtocol(eventTransport);
        final EventService.Client serviceClient = new EventService.Client(eventProtocol);

        Executors.newSingleThreadExecutor().submit
        (new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        CuratorEvent nextEvent = serviceClient.getNextEvent();
                        System.out.println(nextEvent.type);
                    }
                    catch ( TException e )
                    {
                        e.printStackTrace();
                    }
                }
            });

        CuratorProjection curatorProjection = client.newCuratorProjection(new CuratorProjectionSpec());
    }
}
