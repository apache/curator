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

package org.apache.curator.x.websockets.api;

import com.google.common.collect.ImmutableMap;
import org.apache.curator.x.websockets.api.zookeeper.Create;
import java.io.FileNotFoundException;
import java.util.Map;

public class CommandManager
{
    private final Map<String, Class<? extends ApiCommand>> commands;

    public CommandManager()
    {
        ImmutableMap.Builder<String, Class<? extends ApiCommand>> builder = ImmutableMap.builder();

        builder.put("zookeeper/create", Create.class);

        commands = builder.build();
    }

    public ApiCommand newCommand(String name) throws Exception
    {
        Class<? extends ApiCommand> clazz = commands.get(name);
        if ( clazz == null )
        {
            throw new FileNotFoundException(name);
        }
        return clazz.newInstance();
    }
}
