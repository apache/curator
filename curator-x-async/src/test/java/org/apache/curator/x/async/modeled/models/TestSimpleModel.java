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

package org.apache.curator.x.async.modeled.models;

import java.util.Objects;

public class TestSimpleModel {
    private final String name;
    private final int age;

    public TestSimpleModel() {
        this("", 0);
    }

    public TestSimpleModel(String name, int age) {
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.age = Objects.requireNonNull(age, "age cannot be null");
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TestSimpleModel that = (TestSimpleModel) o;

        //noinspection SimplifiableIfStatement
        if (age != that.age) {
            return false;
        }
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + age;
        return result;
    }

    @Override
    public String toString() {
        return "TestSimpleModel{" + "name='" + name + '\'' + ", age=" + age + '}';
    }
}
