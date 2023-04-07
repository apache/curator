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

public class TestNewerModel
{
    private final String firstName;
    private final String lastName;
    private final String address;
    private final int age;
    private final BigInteger salary;
    private final long newField;

    public TestNewerModel()
    {
        this("", "", "", 0, BigInteger.ZERO, 0);
    }

    public TestNewerModel(String firstName, String lastName, String address, int age, BigInteger salary, long newField)
    {
        this.firstName = Objects.requireNonNull(firstName, "firstName cannot be null");
        this.lastName = Objects.requireNonNull(lastName, "lastName cannot be null");
        this.address = Objects.requireNonNull(address, "address cannot be null");
        this.age = Objects.requireNonNull(age, "age cannot be null");
        this.salary = salary;
        this.newField = newField;
    }

    public String getFirstName()
    {
        return firstName;
    }

    public String getLastName()
    {
        return lastName;
    }

    public String getAddress()
    {
        return address;
    }

    public int getAge()
    {
        return age;
    }

    public BigInteger getSalary()
    {
        return salary;
    }

    public long getNewField()
    {
        return newField;
    }

    public boolean equalsOld(TestModel model)
    {
        return firstName.equals(model.getFirstName())
            && lastName.equals(model.getLastName())
            && address.equals(model.getAddress())
            && salary.equals(model.getSalary())
            && age == model.getAge()
            ;
    }

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

        TestNewerModel that = (TestNewerModel)o;

        if ( age != that.age )
        {
            return false;
        }
        if ( newField != that.newField )
        {
            return false;
        }
        if ( !firstName.equals(that.firstName) )
        {
            return false;
        }
        if ( !lastName.equals(that.lastName) )
        {
            return false;
        }
        //noinspection SimplifiableIfStatement
        if ( !address.equals(that.address) )
        {
            return false;
        }
        return salary.equals(that.salary);
    }

    @Override
    public int hashCode()
    {
        int result = firstName.hashCode();
        result = 31 * result + lastName.hashCode();
        result = 31 * result + address.hashCode();
        result = 31 * result + age;
        result = 31 * result + salary.hashCode();
        result = 31 * result + (int)(newField ^ (newField >>> 32));
        return result;
    }
}
