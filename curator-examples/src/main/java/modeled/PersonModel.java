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
package modeled;

import java.util.Objects;

public class PersonModel
{
    private final PersonId id;
    private final ContainerType containerType;
    private final String firstName;
    private final String lastName;
    private final int age;

    public PersonModel()
    {
        this(new PersonId(), new ContainerType(), null, null, 0);
    }

    public PersonModel(PersonId id, ContainerType containerType, String firstName, String lastName, int age)
    {
        this.id = Objects.requireNonNull(id, "id cannot be null");
        this.containerType = Objects.requireNonNull(containerType, "containerType cannot be null");
        this.firstName = Objects.requireNonNull(firstName, "firstName cannot be null");
        this.lastName = Objects.requireNonNull(lastName, "lastName cannot be null");
        this.age = age;
    }

    public PersonId getId()
    {
        return id;
    }

    public ContainerType getContainerType()
    {
        return containerType;
    }

    public String getFirstName()
    {
        return firstName;
    }

    public String getLastName()
    {
        return lastName;
    }

    public int getAge()
    {
        return age;
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

        PersonModel that = (PersonModel)o;

        if ( age != that.age )
        {
            return false;
        }
        if ( !id.equals(that.id) )
        {
            return false;
        }
        if ( !containerType.equals(that.containerType) )
        {
            return false;
        }
        //noinspection SimplifiableIfStatement
        if ( !firstName.equals(that.firstName) )
        {
            return false;
        }
        return lastName.equals(that.lastName);
    }

    @Override
    public int hashCode()
    {
        int result = id.hashCode();
        result = 31 * result + containerType.hashCode();
        result = 31 * result + firstName.hashCode();
        result = 31 * result + lastName.hashCode();
        result = 31 * result + age;
        return result;
    }

    @Override
    public String toString()
    {
        return "PersonModel{" + "id=" + id + ", containerType=" + containerType + ", firstName='" + firstName + '\'' + ", lastName='" + lastName + '\'' + ", age=" + age + '}';
    }
}
