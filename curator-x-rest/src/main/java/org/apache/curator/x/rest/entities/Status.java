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

import com.google.common.collect.Lists;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

@XmlRootElement
public class Status
{
    private String state;
    private List<StatusMessage> messages;

    public Status()
    {
        this("", Lists.<StatusMessage>newArrayList());
    }

    public Status(String state, List<StatusMessage> messages)
    {
        this.state = state;
        this.messages = messages;
    }

    public String getState()
    {
        return state;
    }

    public void setState(String state)
    {
        this.state = state;
    }

    public List<StatusMessage> getMessages()
    {
        return messages;
    }

    public void setMessages(List<StatusMessage> messages)
    {
        this.messages = messages;
    }
}
