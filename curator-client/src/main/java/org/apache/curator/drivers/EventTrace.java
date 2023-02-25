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

public class EventTrace
{
  private final String name;
  private final TracerDriver driver;
  private final long sessionId;

  public EventTrace(String name, TracerDriver driver) {
    this(name, driver, -1);
  }

  public EventTrace(String name, TracerDriver driver, long sessionId) {
    this.name = name;
    this.driver = driver;
    this.sessionId = sessionId;
  }

  public String getName() {
    return this.name;
  }

  public long getSessionId() {
    return this.sessionId;
  }

  public void commit() {
    if (this.driver instanceof AdvancedTracerDriver) {
      ((AdvancedTracerDriver) this.driver).addEvent(this);
    } else {
      this.driver.addCount(this.name, 1);
    }
  }
}

