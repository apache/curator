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
package org.apache.curator.x.rest.entities;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class SemaphoreSpec
{
    private String path;
    private int acquireQty;
    private int maxWaitMs;
    private int maxLeases;

    public SemaphoreSpec()
    {
        this("", 0, 0, 0);
    }

    public SemaphoreSpec(String path, int acquireQty, int maxWaitMs, int maxLeases)
    {
        this.path = path;
        this.acquireQty = acquireQty;
        this.maxWaitMs = maxWaitMs;
        this.maxLeases = maxLeases;
    }

    public int getAcquireQty()
    {
        return acquireQty;
    }

    public void setAcquireQty(int acquireQty)
    {
        this.acquireQty = acquireQty;
    }

    public String getPath()
    {
        return path;
    }

    public void setPath(String path)
    {
        this.path = path;
    }

    public int getMaxWaitMs()
    {
        return maxWaitMs;
    }

    public void setMaxWaitMs(int maxWaitMs)
    {
        this.maxWaitMs = maxWaitMs;
    }

    public int getMaxLeases()
    {
        return maxLeases;
    }

    public void setMaxLeases(int maxLeases)
    {
        this.maxLeases = maxLeases;
    }
}
