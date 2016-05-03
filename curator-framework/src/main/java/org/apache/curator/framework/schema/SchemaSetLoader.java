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
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
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
 *         "dataValidator": "name",              optional - name of a data validator - default is no validator
 *         "documentation": "docs",              optional - user displayable docs - default is ""
 *         "ephemeral": "allowance",             optional - "can", "must" or "cannot" - default is "can"
 *         "sequential": "allowance",            optional - "can", "must" or "cannot" - default is "can"
 *         "watched": "allowance",               optional - "can", "must" or "cannot" - default is "can"
 *         "canBeDeleted": "true/false           optional - true if ZNode at path can be deleted - default is true
 *     }
 * ]
 * </pre></code>
 * </p>
 */
public class SchemaSetLoader
{
    private final Map<SchemaKey, Schema> schemas;

    /**
     * Called to map a data validator name in the JSON stream to an actual data validator
     */
    public interface DataValidatorMapper
    {
        /**
         * @param name name of the validator
         * @return the validator
         */
        DataValidator getDataValidator(String name);
    }

    public SchemaSetLoader(String json, DataValidatorMapper dataValidatorMapper)
    {
        this(new StringReader(json), dataValidatorMapper);
    }

    public SchemaSetLoader(Reader in, DataValidatorMapper dataValidatorMapper)
    {
        ImmutableMap.Builder<SchemaKey, Schema> builder = ImmutableMap.builder();
        try
        {
            JsonNode root = new ObjectMapper().readTree(in);
            read(builder, root, dataValidatorMapper);
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

    private void read(ImmutableMap.Builder<SchemaKey, Schema> builder, JsonNode node, DataValidatorMapper dataValidatorMapper)
    {
        for ( JsonNode child : node )
        {
            readNode(builder, child, dataValidatorMapper);
        }
    }

    private void readNode(ImmutableMap.Builder<SchemaKey, Schema> builder, JsonNode node, DataValidatorMapper dataValidatorMapper)
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

        String dataValidatorName = getText(node, "dataValidator", null);
        if ( dataValidatorName != null )
        {
            if ( dataValidatorMapper == null )
            {
                throw new RuntimeException("No DataValidatorMapper provided but needed at: " + node);
            }
            schemaBuilder.dataValidator(dataValidatorMapper.getDataValidator(dataValidatorName));
        }

        Schema schema = schemaBuilder.documentation(getText(node, "documentation", ""))
            .ephemeral(getAllowance(node, "ephemeral"))
            .sequential(getAllowance(node, "sequential"))
            .watched(getAllowance(node, "watched"))
            .canBeDeleted(getBoolean(node, "canBeDeleted"))
            .build();
        builder.put(new SchemaKey(name), schema);
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
