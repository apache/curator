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

package org.apache.curator.drivers;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

/**
 * Used to trace the metrics of a certain Zookeeper operation.
 */
public class OperationTrace {
    private final String name;
    private final TracerDriver driver;

    private int returnCode = KeeperException.Code.OK.intValue();
    private long latencyMs;
    private long requestBytesLength;
    private long responseBytesLength;
    private String path;
    private boolean withWatcher;
    private long sessionId;
    private Stat stat;

    private final long startTimeNanos = System.nanoTime();

    public OperationTrace(String name, TracerDriver driver) {
        this(name, driver, -1);
    }

    public OperationTrace(String name, TracerDriver driver, long sessionId) {
        this.name = name;
        this.driver = driver;
        this.sessionId = sessionId;
    }

    public OperationTrace setReturnCode(int returnCode) {
        this.returnCode = returnCode;
        return this;
    }

    public OperationTrace setRequestBytesLength(long length) {
        this.requestBytesLength = length;
        return this;
    }

    public OperationTrace setRequestBytesLength(String data) {
        if (data == null) {
            return this;
        }

        try {
            this.setRequestBytesLength(data.getBytes("UTF-8").length);
        } catch (UnsupportedEncodingException e) {
            // Ignore the exception.
        }

        return this;
    }

    public OperationTrace setRequestBytesLength(byte[] data) {
        if (data == null) {
            return this;
        }

        return this.setRequestBytesLength(data.length);
    }

    public OperationTrace setResponseBytesLength(long length) {
        this.responseBytesLength = length;
        return this;
    }

    public OperationTrace setResponseBytesLength(byte[] data) {
        if (data == null) {
            return this;
        }

        return this.setResponseBytesLength(data.length);
    }

    public OperationTrace setPath(String path) {
        this.path = path;
        return this;
    }

    public OperationTrace setWithWatcher(boolean withWatcher) {
        this.withWatcher = withWatcher;
        return this;
    }

    public OperationTrace setStat(Stat stat) {
        this.stat = stat;
        return this;
    }

    public String getName() {
        return this.name;
    }

    public int getReturnCode() {
        return this.returnCode;
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

    public long getSessionId() {
        return this.sessionId;
    }

    public String getPath() {
        return this.path;
    }

    public boolean isWithWatcher() {
        return this.withWatcher;
    }

    public Stat getStat() {
        return this.stat;
    }

    public void commit() {
        long elapsed = System.nanoTime() - startTimeNanos;
        this.latencyMs = TimeUnit.MILLISECONDS.convert(elapsed, TimeUnit.NANOSECONDS);
        if (this.driver instanceof AdvancedTracerDriver) {
            ((AdvancedTracerDriver) this.driver).addTrace(this);
        } else {
            this.driver.addTrace(this.name, elapsed, TimeUnit.NANOSECONDS);
        }
    }
}
