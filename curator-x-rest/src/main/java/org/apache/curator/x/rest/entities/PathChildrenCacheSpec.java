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

import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class PathChildrenCacheSpec
{
    private String path;
    private boolean cacheData;
    private boolean dataIsCompressed;
    private PathChildrenCache.StartMode startMode;

    public PathChildrenCacheSpec()
    {
        this("", false, false, PathChildrenCache.StartMode.NORMAL);
    }

    public PathChildrenCacheSpec(String path, boolean cacheData, boolean dataIsCompressed, PathChildrenCache.StartMode startMode)
    {
        this.path = path;
        this.cacheData = cacheData;
        this.dataIsCompressed = dataIsCompressed;
        this.startMode = startMode;
    }

    public String getPath()
    {
        return path;
    }

    public void setPath(String path)
    {
        this.path = path;
    }

    public boolean isCacheData()
    {
        return cacheData;
    }

    public void setCacheData(boolean cacheData)
    {
        this.cacheData = cacheData;
    }

    public boolean isDataIsCompressed()
    {
        return dataIsCompressed;
    }

    public void setDataIsCompressed(boolean dataIsCompressed)
    {
        this.dataIsCompressed = dataIsCompressed;
    }

    public PathChildrenCache.StartMode getStartMode()
    {
        return startMode;
    }

    public void setStartMode(PathChildrenCache.StartMode startMode)
    {
        this.startMode = startMode;
    }
}
