/*
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

package org.apache.curator.x.async.modeled.versioned;

/**
 * A container for a model instance and a version. Can be used with the
 * {@link org.apache.curator.x.async.modeled.ModeledFramework#versioned()} APIs
 */
@FunctionalInterface
public interface Versioned<T>
{
    /**
     * Returns the contained model
     *
     * @return model
     */
    T model();

    /**
     * Returns the version of the model when it was read
     *
     * @return version
     */
    default int version()
    {
        return -1;
    }

    /**
     * Return a new Versioned wrapper for the given model and version
     *
     * @param model model
     * @param version version
     * @return new Versioned wrapper
     */
    static <T> Versioned<T> from(T model, int version)
    {
        return new Versioned<T>()
        {
            @Override
            public int version()
            {
                return version;
            }

            @Override
            public T model()
            {
                return model;
            }
        };
    }
}
