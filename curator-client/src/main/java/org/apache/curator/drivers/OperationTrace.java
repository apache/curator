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
package org.apache.curator.drivers;

import org.apache.curator.drivers.TracerDriver;

/**
 * Used to trace the metrics of a certain Zookeeper operation.
 */
public class OperationTrace
{
    private final String name;
    private final TracerDriver driver;
    private long latencyMs;
    private long requestBytesLength;
    private long responseBytesLength;

    private final long startTimeMs = System.currentTimeMillis();

    public OperationTrace(String name, TracerDriver driver) {
      this.name = name;
      this.driver = driver;
    }

    public OperationTrace setRequestBytesLength(long length) {
      this.requestBytesLength = length;
      return this;
    }

    public OperationTrace setResponseBytesLength(long length) {
      this.responseBytesLength = length;
      return this;
    }

    public String getName() {
      return this.name;
    }

    public long getLatencyMs() {
      return this.latencyMs;
    }

    public long getRequestBytesLength() {
      return this.requestBytesLength;
    }

    public long getResponseBytesLength() {
      return this.responseBytesLength;
    }

    public void commit() {
      this.latencyMs = System.currentTimeMillis() - startTimeMs;
      this.driver.addTrace(this);
    }
}
