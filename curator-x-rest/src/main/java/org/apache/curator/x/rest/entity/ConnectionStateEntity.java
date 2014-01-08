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
public class ConnectionStateEntity
{
    private String state;
    private long stateCount;

    public ConnectionStateEntity()
    {
        this("", -1);
    }

    public ConnectionStateEntity(String state, long stateCount)
    {
        this.state = state;
        this.stateCount = stateCount;
    }

    public String getState()
    {
        return state;
    }

    public void setState(String state)
    {
        this.state = state;
    }

    public long getStateCount()
    {
        return stateCount;
    }

    public void setStateCount(long stateCount)
    {
        this.stateCount = stateCount;
    }
}
