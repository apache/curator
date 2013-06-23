package org.apache.curator.x.discovery;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class DownInstanceManager
{
    private final long timeoutMs;
    private final DelayQueue<Entry> queue = new DelayQueue<Entry>();

    private class Entry implements Delayed
    {
        private final long startMs = System.currentTimeMillis();
        private final ServiceInstance<?> instance;

        private Entry(ServiceInstance<?> instance)
        {
            this.instance = instance;
        }

        public long getDelay(TimeUnit unit)
        {
            long elapsedMs = System.currentTimeMillis() - startMs;
            long remainingMs = timeoutMs - elapsedMs;
            return (remainingMs > 0) ? TimeUnit.MILLISECONDS.convert(remainingMs, unit) : 0;
        }

        @Override
        // note: note semantically the same as equals()/hashCode()
        public int compareTo(Delayed rhs)
        {
            long diff = getDelay(TimeUnit.MILLISECONDS) - rhs.getDelay(TimeUnit.MILLISECONDS);
            return (diff < 0) ? -1 :((diff > 0) ? 1 : 0);
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

            Entry entry = (Entry)o;

            //noinspection RedundantIfStatement
            if ( !instance.equals(entry.instance) )
            {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            return instance.hashCode();
        }
    }

    public DownInstanceManager(long timeout, TimeUnit unit)
    {
        timeoutMs = unit.toMillis(timeout);
    }

    public void add(ServiceInstance<?> instance)
    {
        queue.add(new Entry(instance));
    }

    public boolean contains(ServiceInstance<?> instance)
    {
        //noinspection StatementWithEmptyBody
        while ( queue.poll() != null ){}    // pull out expired entries
        return queue.contains(new Entry(instance));
    }

    public boolean hasEntries()
    {
        return !queue.isEmpty();
    }
}
