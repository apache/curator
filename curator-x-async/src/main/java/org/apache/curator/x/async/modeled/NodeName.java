package org.apache.curator.x.async.modeled;

/**
 * Used by the various "resolved" methods and "at" methods.
 * If the argument to one of these methods implements this interface,
 * the {@link #nodeName()} method is used instead of calling <code>toString()</code>
 */
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
