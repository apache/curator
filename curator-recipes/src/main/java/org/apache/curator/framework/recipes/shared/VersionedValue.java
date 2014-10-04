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
package org.apache.curator.framework.recipes.shared;

import com.google.common.base.Preconditions;

/**
 * POJO for a version and a value
 */
public class VersionedValue<T>
{
    private final int version;
    private final T value;

    /**
     * @param version the version
     * @param value the value (cannot be null)
     */
    VersionedValue(int version, T value)
    {
        this.version = version;
        this.value = Preconditions.checkNotNull(value, "value cannot be null");
    }

    public int getVersion()
    {
        return version;
    }

    public T getValue()
    {
        return value;
    }
}
