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

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.node.ObjectNode;
import java.io.IOException;
import java.util.UUID;

public class JsonUtils
{
    public static final String FIELD_TYPE = "type";
    public static final String FIELD_ID = "id";
    public static final String FIELD_VALUE = "value";

    public static final String SYSTEM_TYPE_CONNECTION_STATE_CHANGE = "system/connection-state-change";

    public static String newMessage(ObjectMapper mapper, ObjectWriter writer, String type, ObjectNode value) throws IOException
    {
        return newMessage(mapper, writer, type, UUID.randomUUID().toString(), value);
    }

    public static String newMessage(ObjectMapper mapper, ObjectWriter writer, String type, String id, ObjectNode value) throws IOException
    {
        ObjectNode node = mapper.createObjectNode();
        node.put(FIELD_TYPE, type);
        node.put(FIELD_ID, id);
        node.put(FIELD_VALUE, value);
        return writer.writeValueAsString(node);
    }

    public static String getRequiredString(JsonNode node, String name) throws Exception
    {
        JsonNode jsonNode = node.get(name);
        if ( jsonNode == null )
        {
            throw new Exception("Required field is missing: " + name);
        }
        return jsonNode.asText();
    }

    public static String getOptionalString(JsonNode node, String name)
    {
        return getOptionalString(node, name, null);
    }

    public static String getOptionalString(JsonNode node, String name, String defaultValue)
    {
        JsonNode jsonNode = node.get(name);
        return (jsonNode != null) ? jsonNode.asText() : defaultValue;
    }

    public static boolean getOptionalBoolean(JsonNode node, String name)
    {
        JsonNode jsonNode = node.get(name);
        return (jsonNode != null) && jsonNode.asBoolean();
    }

    private JsonUtils()
    {
    }
}
