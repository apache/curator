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

package org.apache.curator.framework.recipes.cache;

import java.util.function.Consumer;

public interface CuratorCacheBuilder
{
    /**
     * @param options any options
     * @return this
     */
    CuratorCacheBuilder withOptions(CuratorCache.Options... options);

    /**
     * Alternate storage to use. If not specified, {@link StandardCuratorCacheStorage#standard()} is used
     *
     * @param storage storage instance to use
     * @return this
     */
    CuratorCacheBuilder withStorage(CuratorCacheStorage storage);

    /**
     * By default any unexpected exception is handled by logging the exception. You can change
     * so that a handler is called instead. Under normal circumstances, this shouldn't be necessary.
     *
     * @param exceptionHandler exception handler to use
     */
    CuratorCacheBuilder withExceptionHandler(Consumer<Exception> exceptionHandler);

    /**
     * Return a new Curator Cache based on the builder methods that have been called
     *
     * @return new Curator Cache
     */
    CuratorCache build();
}