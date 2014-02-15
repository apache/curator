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

import org.apache.zookeeper.CreateMode;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class CreateSpec
{
    private String path;
    private String data;
    private CreateMode mode;
    private boolean async;
    private String asyncId;
    private boolean compressed;
    private boolean creatingParentsIfNeeded;
    private boolean withProtection;

    public CreateSpec()
    {
        this("/", "", CreateMode.PERSISTENT, false, "", false, false, false);
    }

    public CreateSpec(String path, String data, CreateMode mode, boolean async, String asyncId, boolean compressed, boolean creatingParentsIfNeeded, boolean withProtection)
    {
        this.path = path;
        this.data = data;
        this.mode = mode;
        this.async = async;
        this.asyncId = asyncId;
        this.compressed = compressed;
        this.creatingParentsIfNeeded = creatingParentsIfNeeded;
        this.withProtection = withProtection;
    }

    public String getPath()
    {
        return path;
    }

    public String getAsyncId()
    {
        return asyncId;
    }

    public void setAsyncId(String asyncId)
    {
        this.asyncId = asyncId;
    }

    public void setPath(String path)
    {
        this.path = path;
    }

    public String getData()
    {
        return data;
    }

    public void setData(String data)
    {
        this.data = data;
    }

    public CreateMode getMode()
    {
        return mode;
    }

    public void setMode(CreateMode mode)
    {
        this.mode = mode;
    }

    public boolean isAsync()
    {
        return async;
    }

    public void setAsync(boolean async)
    {
        this.async = async;
    }

    public boolean isCompressed()
    {
        return compressed;
    }

    public void setCompressed(boolean compressed)
    {
        this.compressed = compressed;
    }

    public boolean isCreatingParentsIfNeeded()
    {
        return creatingParentsIfNeeded;
    }

    public void setCreatingParentsIfNeeded(boolean creatingParentsIfNeeded)
    {
        this.creatingParentsIfNeeded = creatingParentsIfNeeded;
    }

    public boolean isWithProtection()
    {
        return withProtection;
    }

    public void setWithProtection(boolean withProtection)
    {
        this.withProtection = withProtection;
    }
}
