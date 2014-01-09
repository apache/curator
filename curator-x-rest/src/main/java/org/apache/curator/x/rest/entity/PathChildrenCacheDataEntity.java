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

package org.apache.curator.x.rest.entity;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class PathChildrenCacheDataEntity
{
    private String path;
    private String data;
    private StatEntity stat;

    public PathChildrenCacheDataEntity()
    {
        this("", "", new StatEntity());
    }

    public PathChildrenCacheDataEntity(String path, String data, StatEntity stat)
    {
        this.path = path;
        this.data = data;
        this.stat = stat;
    }

    public String getPath()
    {
        return path;
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

    public StatEntity getStat()
    {
        return stat;
    }

    public void setStat(StatEntity stat)
    {
        this.stat = stat;
    }
}
