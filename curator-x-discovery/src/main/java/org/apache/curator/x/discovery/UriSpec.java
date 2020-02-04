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
package org.apache.curator.x.discovery;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * <p>
 *     An abstraction for specifying a URI for an instance allowing for variable substitutions.
 * </p>
 *
 * <p>
 *     A Uri spec is a string with optional replacement fields. A replacement field begins with
 *     an open brace and ends with a close brace. The value between the braces is the name of the
 *     field. e.g. "{scheme}://foo.com:{port}" has two replacement fields named "scheme" and "port".
 *     Several pre-defined fields are listed as constants in this class (e.g. {@link #FIELD_SCHEME}).
 * </p>
 */
public class UriSpec implements Iterable<UriSpec.Part>
{
    private final Logger            log = LoggerFactory.getLogger(getClass());
    private final List<Part>        parts = Lists.newArrayList();

    /**
     * This defaults to "http". If a {@link ServiceInstance} is passed when building and an sslPort
     * is specified in the instance, the replacement is "https".
     */
    public static final String      FIELD_SCHEME = "scheme";

    /**
     * If a {@link ServiceInstance} is passed when building, the replacement is {@link ServiceInstance#getName()}
     */
    public static final String      FIELD_NAME = "name";

    /**
     * If a {@link ServiceInstance} is passed when building, the replacement is {@link ServiceInstance#getId()}
     */
    public static final String      FIELD_ID = "id";

    /**
     * If a {@link ServiceInstance} is passed when building, the replacement is {@link ServiceInstance#getAddress()}
     */
    public static final String      FIELD_ADDRESS = "address";

    /**
     * If a {@link ServiceInstance} is passed when building, the replacement is {@link ServiceInstance#getPort()}
     */
    public static final String      FIELD_PORT = "port";

    /**
     * If a {@link ServiceInstance} is passed when building, the replacement is {@link ServiceInstance#getSslPort()}
     */
    public static final String      FIELD_SSL_PORT = "ssl-port";

    /**
     * If a {@link ServiceInstance} is passed when building, the replacement is {@link ServiceInstance#getRegistrationTimeUTC()}
     */
    public static final String      FIELD_REGISTRATION_TIME_UTC = "registration-time-utc";

    /**
     * If a {@link ServiceInstance} is passed when building, the replacement is {@link ServiceInstance#getServiceType()}
     */
    public static final String      FIELD_SERVICE_TYPE = "service-type";

    /**
     * Always replaced with '{' - i.e. this is how to insert a literal '{'
     */
    public static final String      FIELD_OPEN_BRACE = "[";

    /**
     * Always replaced with '}' - i.e. this is how to insert a literal '}'
     */
    public static final String      FIELD_CLOSE_BRACE = "]";

    /**
     * Represents one token in the Uri spec
     */
    public static class Part
    {
        private final String        value;
        private final boolean       variable;

        /**
         * @param value the token value
         * @param isVariable if true, a replacement field. If false, a literal string
         */
        public Part(String value, boolean isVariable)
        {
            this.value = value;
            this.variable = isVariable;
        }

        public Part()
        {
            value = "";
            variable = false;
        }

        public String getValue()
        {
            return value;
        }

        public boolean isVariable()
        {
            return variable;
        }

        @SuppressWarnings("RedundantIfStatement")
        @Override
        public boolean equals(Object o)
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            Part part = (Part)o;

            if ( variable != part.variable )
            {
                return false;
            }
            if ( !value.equals(part.value) )
            {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = value.hashCode();
            result = 31 * result + (variable ? 1 : 0);
            return result;
        }
    }

    public UriSpec()
    {
        // NOP
    }

    /**
     * @param rawSpec the spec to parse
     */
    public UriSpec(String rawSpec)
    {
        boolean             isInsideVariable = false;
        StringTokenizer     tokenizer = new StringTokenizer(rawSpec, "{}", true);
        while ( tokenizer.hasMoreTokens() )
        {
            String  token = tokenizer.nextToken();
            if ( token.equals("{") )
            {
                Preconditions.checkState(!isInsideVariable, "{ is not allowed inside of a variable specification");
                isInsideVariable = true;
            }
            else if ( token.equals("}") )
            {
                Preconditions.checkState(isInsideVariable, "} must be preceded by {");
                isInsideVariable = false;
            }
            else
            {
                if ( isInsideVariable )
                {
                    token = token.trim();
                }
                add(new Part(token, isInsideVariable));
            }
        }

        Preconditions.checkState(!isInsideVariable, "Final variable not closed - expected }");
    }

    /**
     * Build into a UriSpec string
     *
     * @return UriSpec string
     */
    public String   build()
    {
        return build(null, Maps.<String, Object>newHashMap());
    }

    /**
     * Build into a UriSpec string
     *
     * @param serviceInstance instance to use for pre-defined replacement fields
     * @return UriSpec string
     */
    public String   build(ServiceInstance<?> serviceInstance)
    {
        return build(serviceInstance, Maps.<String, Object>newHashMap());
    }

    /**
     * Build into a UriSpec string
     *
     * @param variables a mapping of field replacement names to values. Note: any fields listed
     *                  in this map override pre-defined fields
     * @return UriSpec string
     */
    public String   build(Map<String, Object> variables)
    {
        return build(null, variables);
    }

    /**
     * Build into a UriSpec string
     *
     * @param serviceInstance instance to use for pre-defined replacement fields
     * @param variables a mapping of field replacement names to values. Note: any fields listed
     *                  in this map override pre-defined fields
     * @return UriSpec string
     */
    public String   build(ServiceInstance<?> serviceInstance, Map<String, Object> variables)
    {
        Map<String, Object>     localVariables = Maps.newHashMap();
        localVariables.put(FIELD_OPEN_BRACE, "{");
        localVariables.put(FIELD_CLOSE_BRACE, "}");
        localVariables.put(FIELD_SCHEME, "http");

        if ( serviceInstance != null )
        {
            localVariables.put(FIELD_NAME, nullCheck(serviceInstance.getName()));
            localVariables.put(FIELD_ID, nullCheck(serviceInstance.getId()));
            localVariables.put(FIELD_ADDRESS, nullCheck(serviceInstance.getAddress()));
            localVariables.put(FIELD_PORT, nullCheck(serviceInstance.getPort()));
            localVariables.put(FIELD_SSL_PORT, nullCheck(serviceInstance.getSslPort()));
            localVariables.put(FIELD_REGISTRATION_TIME_UTC, nullCheck(serviceInstance.getRegistrationTimeUTC()));
            localVariables.put(FIELD_SERVICE_TYPE, (serviceInstance.getServiceType() != null) ? serviceInstance.getServiceType().name().toLowerCase() : "");
            if ( serviceInstance.getSslPort() != null )
            {
                localVariables.put(FIELD_SCHEME, "https");
            }
        }

        localVariables.putAll(variables);

        StringBuilder       str = new StringBuilder();
        for ( Part p : parts )
        {
            if ( p.isVariable() )
            {
                Object value = localVariables.get(p.getValue());
                if ( value == null )
                {
                    log.debug("Variable not found: {}", p.getValue());
                }
                else
                {
                    str.append(value);
                }
            }
            else
            {
                str.append(p.getValue());
            }
        }

        return str.toString();
    }

    @Override
    public Iterator<Part> iterator()
    {
        return Iterators.unmodifiableIterator(parts.iterator());
    }

    /**
     * @return the parts
     */
    public List<Part>   getParts()
    {
        return ImmutableList.copyOf(parts);
    }

    /**
     * Add a part to the end of the list
     *
     * @param part part to add
     */
    public void     add(Part part)
    {
        parts.add(part);
    }

    /**
     * Remove the given part
     *
     * @param part the part
     */
    public void     remove(Part part)
    {
        parts.remove(part);
    }

    @SuppressWarnings("RedundantIfStatement")
    @Override
    public boolean equals(Object o)
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        UriSpec spec = (UriSpec)o;

        if ( !parts.equals(spec.parts) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return parts.hashCode();
    }

    private Object nullCheck(Object o)
    {
        return (o != null) ? o : "";
    }
}
