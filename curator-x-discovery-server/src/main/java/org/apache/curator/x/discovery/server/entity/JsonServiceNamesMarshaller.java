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
package org.apache.curator.x.discovery.server.entity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.List;

/**
 * Message body reader/writer. Inject this as appropriate for the JAX-RS implementation you are using
 */
@Provider
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class JsonServiceNamesMarshaller implements MessageBodyReader<ServiceNames>, MessageBodyWriter<ServiceNames>
{
    @Override
    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType)
    {
        return allow(type, mediaType);
    }

    @Override
    public ServiceNames readFrom(Class<ServiceNames> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, String> httpHeaders, InputStream entityStream) throws IOException, WebApplicationException
    {
        List<String>        names = Lists.newArrayList();
        ObjectMapper        mapper = new ObjectMapper();
        JsonNode            tree = mapper.reader().readTree(entityStream);
        for ( int i = 0; i < tree.size(); ++i )
        {
            JsonNode node = tree.get(i);
            names.add(node.get("name").asText());
        }
        return new ServiceNames(names);
    }

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType)
    {
        return allow(type, mediaType);
    }

    @Override
    public long getSize(ServiceNames serviceNames, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType)
    {
        return -1;
    }

    @Override
    public void writeTo(ServiceNames serviceNames, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) throws IOException, WebApplicationException
    {
        ObjectMapper        mapper = new ObjectMapper();
        ArrayNode           arrayNode = mapper.createArrayNode();
        for ( String name : serviceNames.getNames() )
        {
            ObjectNode      node = mapper.createObjectNode();
            node.put("name", name);
            arrayNode.add(node);
        }

        mapper.writer().writeValue(entityStream, arrayNode);
    }

    private static boolean allow(Class<?> type, MediaType mediaType)
    {
        return ServiceNames.class.isAssignableFrom(type) && mediaType.equals(MediaType.APPLICATION_JSON_TYPE);
    }
}
