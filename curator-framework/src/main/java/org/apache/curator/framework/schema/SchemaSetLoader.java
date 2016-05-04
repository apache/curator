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
package org.apache.curator.framework.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * <p>
 *     Utility to load schems set from a JSON stream/file. <strong>NOTE:</strong>
 *     to avoid adding a new dependency to Curator, the Jackson library has been used
 *     with "provided" scope. You will need to add a dependency to <code>com.fasterxml.jackson.core:jackson-core:2.7.3</code>
 *     and <code>com.fasterxml.jackson.core:jackson-databind:2.7.3</code> to your project
 * </p>
 *
 * <p>
 *     The JSON stream should be an array of named schemas:<br><br>
 * <code><pre>
 * [
 *     {
 *         "name": "name",                       required - name of the schema
 *         "path": "path or pattern",            required - full path or regex pattern
 *         "isRegex": true/false,                optional - true if path is a regular expression - default is false
 *         "schemaValidator": "name",            optional - name of a schema validator - default is no validator
 *         "documentation": "docs",              optional - user displayable docs - default is ""
 *         "ephemeral": "allowance",             optional - "can", "must" or "cannot" - default is "can"
 *         "sequential": "allowance",            optional - "can", "must" or "cannot" - default is "can"
 *         "watched": "allowance",               optional - "can", "must" or "cannot" - default is "can"
 *         "canBeDeleted": true/false            optional - true if ZNode at path can be deleted - default is true
 *         "metadata": {                         optional - any fields -> values that you want
 *             "field1": "value1",
 *             "field2": "value2"
 *         }
 *     }
 * ]
 * </pre></code>
 * </p>
 */
public class SchemaSetLoader
{
    private final List<Schema> schemas;

    /**
     * Called to map a schema validator name in the JSON stream to an actual data validator
     */
    public interface SchemaValidatorMapper
    {
        /**
         * @param name name of the validator
         * @return the validator
         */
        SchemaValidator getSchemaValidator(String name);
    }

    public SchemaSetLoader(String json, SchemaValidatorMapper schemaValidatorMapper)
    {
        this(new StringReader(json), schemaValidatorMapper);
    }

    public SchemaSetLoader(Reader in, SchemaValidatorMapper schemaValidatorMapper)
    {
        ImmutableList.Builder<Schema> builder = ImmutableList.builder();
        try
        {
            JsonNode root = new ObjectMapper().readTree(in);
            read(builder, root, schemaValidatorMapper);
        }
        catch ( IOException e )
        {
            throw new RuntimeException(e);
        }
        schemas = builder.build();
    }

    public SchemaSet toSchemaSet(boolean useDefaultSchema)
    {
        return new SchemaSet(schemas, useDefaultSchema);
    }

    private void read(ImmutableList.Builder<Schema> builder, JsonNode node, SchemaValidatorMapper schemaValidatorMapper)
    {
        for ( JsonNode child : node )
        {
            readNode(builder, child, schemaValidatorMapper);
        }
    }

    private void readNode(ImmutableList.Builder<Schema> builder, JsonNode node, SchemaValidatorMapper schemaValidatorMapper)
    {
        String name = getText(node, "name", null);
        String path = getText(node, "path", null);
        boolean isRegex = getBoolean(node, "isRegex");
        if ( name == null )
        {
            throw new RuntimeException("name is required at: " + node);
        }
        if ( path == null )
        {
            throw new RuntimeException("path is required at: " + node);
        }

        SchemaBuilder schemaBuilder = isRegex ? Schema.builder(Pattern.compile(path)) : Schema.builder(path);

        String schemaValidatorName = getText(node, "schemaValidator", null);
        if ( schemaValidatorName != null )
        {
            if ( schemaValidatorMapper == null )
            {
                throw new RuntimeException("No SchemaValidatorMapper provided but needed at: " + node);
            }
            schemaBuilder.dataValidator(schemaValidatorMapper.getSchemaValidator(schemaValidatorName));
        }

        Map<String, String> metadata = Maps.newHashMap();
        if ( node.has("metadata") )
        {
            JsonNode metadataNode = node.get("metadata");
            Iterator<String> fieldNameIterator = metadataNode.fieldNames();
            while ( fieldNameIterator.hasNext() )
            {
                String fieldName = fieldNameIterator.next();
                metadata.put(fieldName, getText(metadataNode, fieldName, ""));
            }
        }

        Schema schema = schemaBuilder.name(name)
            .documentation(getText(node, "documentation", ""))
            .ephemeral(getAllowance(node, "ephemeral"))
            .sequential(getAllowance(node, "sequential"))
            .watched(getAllowance(node, "watched"))
            .canBeDeleted(getBoolean(node, "canBeDeleted"))
            .metadata(metadata)
            .build();
        builder.add(schema);
    }

    private String getText(JsonNode node, String name, String defaultValue)
    {
        JsonNode namedNode = node.get(name);
        return (namedNode != null) ? namedNode.asText() : defaultValue;
    }

    private boolean getBoolean(JsonNode node, String name)
    {
        JsonNode namedNode = node.get(name);
        return (namedNode != null) && namedNode.asBoolean();
    }

    private Schema.Allowance getAllowance(JsonNode node, String name)
    {
        JsonNode namedNode = node.get(name);
        try
        {
            return (namedNode != null) ? Schema.Allowance.valueOf(namedNode.asText().toUpperCase()) : Schema.Allowance.CAN;
        }
        catch ( IllegalArgumentException ignore )
        {
            throw new RuntimeException("Must be one of: " + Arrays.toString(Schema.Allowance.values()) + " at " + node);
        }
    }
}
