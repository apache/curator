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

package org.apache.curator.x.discovery.details;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.junit.jupiter.api.Test;

public class DiscoveryPathConstructorImplTest {
    @Test
    public void testCanonicalDiscoveryPathConstructor() {
        assertThatThrownBy(() -> new DiscoveryPathConstructorImpl(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DiscoveryPathConstructorImpl("")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DiscoveryPathConstructorImpl("foo/bar"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DiscoveryPathConstructorImpl("foo/bar/"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DiscoveryPathConstructorImpl("/foo/bar/"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testGetBasePath() {
        assertThat(new DiscoveryPathConstructorImpl("/foo/bar").getBasePath()).isEqualTo("/foo/bar");
        assertThat(new DiscoveryPathConstructorImpl("/").getBasePath()).isEqualTo("/");
    }

    @Test
    public void testGetPathForInstances() {
        DiscoveryPathConstructorImpl constructor = new DiscoveryPathConstructorImpl("/foo/bar");
        assertThat(constructor.getPathForInstances("baz")).isEqualTo("/foo/bar/baz");
        assertThat(constructor.getPathForInstances("")).isEqualTo("/foo/bar");
        assertThat(constructor.getPathForInstances(null)).isEqualTo("/foo/bar");
    }

    @Test
    public void testGetPathForInstance() {
        DiscoveryPathConstructorImpl constructor = new DiscoveryPathConstructorImpl("/foo");
        assertThat(constructor.getPathForInstance("bar", "baz")).isEqualTo("/foo/bar/baz");
        assertThat(constructor.getPathForInstance("", "baz")).isEqualTo("/foo/baz");
        assertThat(constructor.getPathForInstance(null, "baz")).isEqualTo("/foo/baz");
        assertThat(constructor.getPathForInstance("bar", "")).isEqualTo("/foo/bar");
        assertThat(constructor.getPathForInstance("bar", null)).isEqualTo("/foo/bar");
        assertThat(constructor.getPathForInstance("", "")).isEqualTo("/foo");
        assertThat(constructor.getPathForInstance(null, null)).isEqualTo("/foo");
    }
}
