package org.apache.curator.x.async.modeled.models;

import java.util.Objects;

public class TestSimpleModel
{
    private final String name;
    private final int age;

    public TestSimpleModel()
    {
        this("", 0);
    }

    public TestSimpleModel(String name, int age)
    {
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.age = Objects.requireNonNull(age, "age cannot be null");
    }

    public String getName()
    {
        return name;
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

        TestSimpleModel that = (TestSimpleModel)o;

        //noinspection SimplifiableIfStatement
        if ( age != that.age )
        {
            return false;
        }
        return name.equals(that.name);
    }

    @Override
    public int hashCode()
    {
        int result = name.hashCode();
        result = 31 * result + age;
        return result;
    }

    @Override
    public String toString()
    {
        return "TestSimpleModel{" + "name='" + name + '\'' + ", age=" + age + '}';
    }
}
