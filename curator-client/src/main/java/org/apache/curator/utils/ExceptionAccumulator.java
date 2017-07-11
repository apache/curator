package org.apache.curator.utils;

import com.google.common.base.Throwables;

/**
 * Utility to accumulate multiple potential exceptions into one that
 * is thrown at the end
 */
public class ExceptionAccumulator
{
    private volatile Throwable mainEx = null;

    /**
     * If there is an accumulated exception, throw it
     */
    public void propagate()
    {
        if ( mainEx != null )
        {
            Throwables.propagate(mainEx);
        }
    }

    /**
     * Add an exception into the accumulated exceptions. Note:
     * if the exception is {@link java.lang.InterruptedException}
     * then <code>Thread.currentThread().interrupt()</code> is called.
     *
     * @param e the exception
     */
    public void add(Throwable e)
    {
        if ( e instanceof InterruptedException )
        {
            if ( mainEx != null )
            {
                e.addSuppressed(mainEx);
            }
            Thread.currentThread().interrupt();
        }

        if ( mainEx == null )
        {
            mainEx = e;
        }
        else
        {
            mainEx.addSuppressed(e);
        }
    }
}
