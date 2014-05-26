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

import org.apache.curator.generated.CreateSpec;
import org.apache.curator.generated.CuratorProjection;
import org.apache.curator.generated.CuratorProjectionSpec;
import org.apache.curator.generated.CuratorService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;

public class TestClient
{
    public static void main(String[] args) throws TException
    {
        TSocket transport = new TSocket("localhost", 8899);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        CuratorService.Client client = new CuratorService.Client(protocol);
        CuratorProjection curatorProjection = client.newCuratorProjection(new CuratorProjectionSpec());
        System.out.println(curatorProjection);

        CreateSpec createSpec = new CreateSpec();
        createSpec.setPath("/a/b/c");
        createSpec.setCreatingParentsIfNeeded(true);
        createSpec.setData("");
        String s = client.create(curatorProjection, createSpec);
        System.out.println(s);
    }
}
