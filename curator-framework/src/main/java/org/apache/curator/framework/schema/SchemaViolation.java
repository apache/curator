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

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import org.apache.zookeeper.data.ACL;
import java.util.Arrays;
import java.util.List;

/**
 * Thrown by the various <code>validation</code> methods in a Schema
 */
public class SchemaViolation extends RuntimeException
{
    private final Schema schema;
    private final String violation;
    private final ViolatorData violatorData;

    /**
     * Data about the calling API that violated the schema
     */
    public static class ViolatorData
    {
        private final String path;
        private final byte[] data;
        private final List<ACL> acl;

        public ViolatorData(String path, byte[] data, List<ACL> acl)
        {
            this.path = path;
            this.data = (data != null) ? Arrays.copyOf(data, data.length) : null;
            this.acl = (acl != null) ? ImmutableList.copyOf(acl) : null;
        }

        /**
         * The path used in the API or <code>null</code>
         *
         * @return path or null
         */
        public String getPath()
        {
            return path;
        }

        /**
         * The data used in the API or <code>null</code>
         *
         * @return data or null
         */
        public byte[] getData()
        {
            return data;
        }

        /**
         * The ACLs used in the API or <code>null</code>
         *
         * @return ACLs or null
         */
        public List<ACL> getAcl()
        {
            return acl;
        }

        @Override
        public String toString()
        {
            String dataString = (data != null) ? new String(data) : "";
            return "ViolatorData{" + "path='" + path + '\'' + ", data=" + dataString + ", acl=" + acl + '}';
        }
    }

    /**
     * @param violation the violation
     * @deprecated use {@link #SchemaViolation(Schema, ViolatorData, String)} instance
     */
    public SchemaViolation(String violation)
    {
        super(String.format("Schema violation: %s", violation));
        this.schema = null;
        this.violation = violation;
        this.violatorData = new ViolatorData(null, null, null);
    }

    /**
     * @param schema the schema
     * @param violation the violation
     * @deprecated use {@link #SchemaViolation(Schema, ViolatorData, String)} instance
     */
    public SchemaViolation(Schema schema, String violation)
    {
        super(String.format("Schema violation: %s for schema: %s", violation, schema));
        this.schema = schema;
        this.violation = violation;
        this.violatorData = new ViolatorData(null, null, null);
    }

    /**
     * @param schema the schema
     * @param violatorData data about the caller who violated the schema
     * @param violation the violation
     */
    public SchemaViolation(Schema schema, ViolatorData violatorData, String violation)
    {
        super(toString(schema, violation, violatorData));
        this.schema = schema;
        this.violation = violation;
        this.violatorData = violatorData;
    }

    public Schema getSchema()
    {
        return schema;
    }

    public String getViolation()
    {
        return violation;
    }

    public ViolatorData getViolatorData()
    {
        return violatorData;
    }

    @Override
    public String toString()
    {
        return toString(schema, violation, violatorData) + super.toString();
    }

    private static String toString(Schema schema, String violation, ViolatorData violatorData)
    {
        return MoreObjects.firstNonNull(violation, "") + " " + schema + " " + violatorData;
    }
}
