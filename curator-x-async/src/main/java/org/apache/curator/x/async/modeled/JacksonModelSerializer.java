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
package org.apache.curator.x.async.modeled;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.io.IOException;
import java.util.Arrays;

/**
 * Model serializer that uses Jackson for JSON serialization. <strong>IMPORTANT: </strong>
 * the jackson dependency is specified as <code>provided</code> in the curator-x-async Maven POM
 * file to avoid adding a new dependency to Curator. Therefore, if you wish to use the
 * JacksonModelSerializer you must manually add the dependency to your build system.
 */
public class JacksonModelSerializer<T> implements ModelSerializer<T>
{
    private static final ObjectMapper mapper = new ObjectMapper();
    static
    {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private final ObjectReader reader;
    private final ObjectWriter writer;

    public static <T> JacksonModelSerializer<T> build(Class<T> modelClass)
    {
        return new JacksonModelSerializer<>(modelClass);
    }

    public static <T> JacksonModelSerializer<T> build(JavaType type)
    {
        return new JacksonModelSerializer<>(type);
    }

    public static <T> JacksonModelSerializer<T> build(TypeReference type)
    {
        return new JacksonModelSerializer<>(type);
    }

    public JacksonModelSerializer(Class<T> modelClass)
    {
        this(mapper.getTypeFactory().constructType(modelClass));
    }

    public JacksonModelSerializer(JavaType type)
    {
        reader = mapper.readerFor(type);
        writer = mapper.writerFor(type);
    }

    public JacksonModelSerializer(TypeReference type)
    {
        reader = mapper.readerFor(type);
        writer = mapper.writerFor(type);
    }

    @Override
    public byte[] serialize(T model)
    {
        try
        {
            return writer.writeValueAsBytes(model);
        }
        catch ( JsonProcessingException e )
        {
            throw new RuntimeException(String.format("Could not serialize value: %s", model), e);
        }
    }

    @Override
    public T deserialize(byte[] bytes)
    {
        try
        {
            return reader.readValue(bytes);
        }
        catch ( IOException e )
        {
            throw new RuntimeException(String.format("Could not deserialize value: %s", Arrays.toString(bytes)), e);
        }
    }
}
