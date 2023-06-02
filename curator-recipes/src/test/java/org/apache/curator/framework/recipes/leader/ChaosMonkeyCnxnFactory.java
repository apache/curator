/*
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

package org.apache.curator.framework.recipes.leader;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.curator.test.Compatibility;
import org.apache.curator.test.TestingZooKeeperMain;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperServerShutdownHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A connection factory that will behave like the NIOServerCnxnFactory except that
 * it will unexpectedly close the connection right after the <b>first</b> znode has
 * been created in Zookeeper.
 * Subsequent create operations will succeed.
 */
public class ChaosMonkeyCnxnFactory extends NIOServerCnxnFactory {
    public static final String CHAOS_ZNODE = "/mylock";
    public static final String CHAOS_ZNODE_PREFIX = CHAOS_ZNODE + "/";

    private static final Logger log = LoggerFactory.getLogger(ChaosMonkeyCnxnFactory.class);

    /* How long after the first error, connections are rejected */
    public static final long LOCKOUT_DURATION_MS = 6000;

    @Override
    public void startup(ZooKeeperServer zks) throws IOException, InterruptedException {
        super.startup(new ChaosMonkeyZookeeperServer(zks));
    }

    public static class ChaosMonkeyZookeeperServer extends ZooKeeperServer {
        private final ZooKeeperServer zks;
        private long firstError = 0;

        public ChaosMonkeyZookeeperServer(ZooKeeperServer zks) {
            this.zks = zks;
            setTxnLogFactory(zks.getTxnLogFactory());
            setTickTime(zks.getTickTime());
            setMinSessionTimeout(zks.getMinSessionTimeout());
            setMaxSessionTimeout(zks.getMaxSessionTimeout());
        }

        @Override
        public void startup() {
            super.startup();
            if (zks instanceof TestingZooKeeperMain.TestZooKeeperServer) {
                ((TestingZooKeeperMain.TestZooKeeperServer) zks).noteStartup();
            }
        }

        @Override
        public void submitRequest(Request si) {
            long remaining = firstError != 0 ? LOCKOUT_DURATION_MS - (System.currentTimeMillis() - firstError) : 0;
            if (si.type != ZooDefs.OpCode.createSession
                    && si.type != ZooDefs.OpCode.sync
                    && si.type != ZooDefs.OpCode.ping
                    && firstError != 0
                    && remaining > 0) {
                log.debug("Rejected : " + si.toString());
                // Still reject request
                log.debug("Still not ready for " + remaining + "ms");
                Compatibility.serverCnxnClose(si.cnxn);
                return;
            }
            // Submit the request to the legacy Zookeeper server
            log.debug("Applied : " + si.toString());
            super.submitRequest(si);
            // Raise an error if a lock is created
            if ((si.type == ZooDefs.OpCode.create) || (si.type == ZooDefs.OpCode.create2)) {
                CreateRequest createRequest = new CreateRequest();
                try {
                    ByteBuffer duplicate = si.request.duplicate();
                    duplicate.rewind();
                    ByteBufferInputStream.byteBuffer2Record(duplicate, createRequest);
                    if (createRequest.getPath().startsWith(CHAOS_ZNODE_PREFIX) && firstError == 0) {
                        firstError = System.currentTimeMillis();
                        // The znode has been created, close the connection and don't tell it to client
                        log.warn("Closing connection right after " + createRequest.getPath() + " creation");
                        Compatibility.serverCnxnClose(si.cnxn);
                    }
                } catch (Exception e) {
                    // Should not happen
                    Compatibility.serverCnxnClose(si.cnxn);
                }
            }
        }

        @Override
        public ZooKeeperServerShutdownHandler getZkShutdownHandler() {
            return zks.getZkShutdownHandler();
        }
    }
}
