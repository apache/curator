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

import java.math.BigInteger;
import java.util.Objects;

public class TestModel {
    private final String firstName;
    private final String lastName;
    private final String address;
    private final int age;
    private final BigInteger salary;

    public TestModel() {
        this("", "", "", 0, BigInteger.ZERO);
    }

    public TestModel(String firstName, String lastName, String address, int age, BigInteger salary) {
        this.firstName = Objects.requireNonNull(firstName, "firstName cannot be null");
        this.lastName = Objects.requireNonNull(lastName, "lastName cannot be null");
        this.address = Objects.requireNonNull(address, "address cannot be null");
        this.age = Objects.requireNonNull(age, "age cannot be null");
        this.salary = salary;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public String getAddress() {
        return address;
    }

    public int getAge() {
        return age;
    }

    public BigInteger getSalary() {
        return salary;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TestModel testModel = (TestModel) o;

        if (age != testModel.age) {
            return false;
        }
        if (!firstName.equals(testModel.firstName)) {
            return false;
        }
        if (!lastName.equals(testModel.lastName)) {
            return false;
        }
        //noinspection SimplifiableIfStatement
        if (!address.equals(testModel.address)) {
            return false;
        }
        return salary.equals(testModel.salary);
    }

    @Override
    public int hashCode() {
        int result = firstName.hashCode();
        result = 31 * result + lastName.hashCode();
        result = 31 * result + address.hashCode();
        result = 31 * result + age;
        result = 31 * result + salary.hashCode();
        return result;
    }
}
