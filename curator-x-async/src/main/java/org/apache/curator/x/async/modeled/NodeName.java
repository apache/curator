package org.apache.curator.x.async.modeled;

@FunctionalInterface
public interface NodeName
{
    String nodeName();

    static String nameFrom(Object obj)
    {
        if ( obj instanceof NodeName )
        {
            return ((NodeName)obj).nodeName();
        }
        return String.valueOf(obj);
    }
}
