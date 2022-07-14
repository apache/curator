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


package org.apache.curator.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.lang.reflect.Field;

import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.embedded.ZooKeeperServerEmbedded;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestTestingServer {

   @TempDir
   File zkTmpDir;

   @Test
   public void setCustomTickTimeTest() throws Exception {
      final int defaultZkTickTime = ZooKeeperServer.DEFAULT_TICK_TIME;
      final int customTickMs;
      if (defaultZkTickTime > 0) {
         customTickMs = defaultZkTickTime + (defaultZkTickTime == Integer.MAX_VALUE ? -1 : +1);
      } else {
         customTickMs = 100;
      }
      final InstanceSpec spec = new InstanceSpec(zkTmpDir, -1, -1, -1, true, -1, customTickMs, -1);
      final int zkTickTime;
      try (TestingServer testingServer = new TestingServer(spec, true)) {
         ZooKeeperMainFace main = testingServer.getTestingZooKeeperServer().getMain();
         if (main instanceof TestingZooKeeperMain) {
            TestingZooKeeperMain testingZooKeeperMain = (TestingZooKeeperMain) main;
            zkTickTime = testingZooKeeperMain.getZkServer().getTickTime();
         } else if (main instanceof ZooKeeperEmbeddedRunner) {
            ZooKeeperEmbeddedRunner testingZooKeeperMain = (ZooKeeperEmbeddedRunner) main;
            ZooKeeperServerEmbedded zooKeeperEmbedded = testingZooKeeperMain.getZooKeeperEmbedded();
            Field configField = zooKeeperEmbedded.getClass().getDeclaredField("config");
            configField.setAccessible(true);
            QuorumPeerConfig config = (QuorumPeerConfig) configField.get(zooKeeperEmbedded);
            zkTickTime = config.getTickTime();
         } else {
            fail("unsupported main " + main.getClass());
            zkTickTime = -1;
         }
      }
      assertEquals(customTickMs, zkTickTime);
   }
}
