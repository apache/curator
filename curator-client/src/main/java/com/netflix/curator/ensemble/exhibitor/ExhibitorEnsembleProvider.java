/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.curator.ensemble.exhibitor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.curator.RetryLoop;
import com.netflix.curator.RetryPolicy;
import com.netflix.curator.ensemble.EnsembleProvider;
import com.netflix.curator.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Ensemble provider that polls a cluster of Exhibitor (https://github.com/Netflix/exhibitor)
 * instances for the connection string.
 * If the set of instances should change, new ZooKeeper connections will use the new connection
 * string.
 */
public class ExhibitorEnsembleProvider implements EnsembleProvider
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final AtomicReference<Exhibitors> exhibitors = new AtomicReference<Exhibitors>();
    private final AtomicReference<Exhibitors> masterExhibitors = new AtomicReference<Exhibitors>();
    private final ExhibitorRestClient restClient;
    private final String restUriPath;
    private final int pollingMs;
    private final RetryPolicy retryPolicy;
    private final ScheduledExecutorService service = ThreadUtils.newSingleThreadScheduledExecutor("ExhibitorEnsembleProvider");
    private final Random random = new Random();
    private final AtomicReference<String> connectionString = new AtomicReference<String>("");
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);

    private static final String MIME_TYPE = "application/x-www-form-urlencoded";

    private static final String VALUE_PORT = "port";
    private static final String VALUE_COUNT = "count";
    private static final String VALUE_SERVER_PREFIX = "server";

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    /**
     * @param exhibitors the current set of exhibitor instances (can be changed later via {@link #setExhibitors(Exhibitors)})
     * @param restClient the rest client to use (use {@link DefaultExhibitorRestClient} for most cases)
     * @param restUriPath the path of the REST call used to get the server set. Usually: <code>/exhibitor/v1/cluster/list</code>
     * @param pollingMs how ofter to poll the exhibitors for the list
     * @param retryPolicy retry policy to use when connecting to the exhibitors
     */
    public ExhibitorEnsembleProvider(Exhibitors exhibitors, ExhibitorRestClient restClient, String restUriPath, int pollingMs, RetryPolicy retryPolicy)
    {
        this.exhibitors.set(exhibitors);
        this.masterExhibitors.set(exhibitors);
        this.restClient = restClient;
        this.restUriPath = restUriPath;
        this.pollingMs = pollingMs;
        this.retryPolicy = retryPolicy;
    }

    /**
     * Change the set of exhibitors to poll
     *
     * @param newExhibitors new set
     */
    public void     setExhibitors(Exhibitors newExhibitors)
    {
        exhibitors.set(newExhibitors);
        masterExhibitors.set(newExhibitors);
    }

    /**
     * Can be called prior to starting the Curator instance to set the current connection string
     *
     * @throws Exception errors
     */
    public void     pollForInitialEnsemble() throws Exception
    {
        Preconditions.checkState(state.get() == State.LATENT, "Cannot be called after start()");
        poll();
    }

    @Override
    public void start() throws Exception
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Cannot be started more than once");

        service.scheduleWithFixedDelay
        (
            new Runnable()
            {
                @Override
                public void run()
                {
                    poll();
                }
            },
            pollingMs,
            pollingMs,
            TimeUnit.MILLISECONDS
        );
    }

    @Override
    public void close() throws IOException
    {
        Preconditions.checkState(state.compareAndSet(State.STARTED, State.CLOSED), "Already closed or has not been started");

        service.shutdownNow();
    }

    @Override
    public String getConnectionString()
    {
        return connectionString.get();
    }

    @VisibleForTesting
    protected void poll()
    {
        Exhibitors              localExhibitors = exhibitors.get();
        Map<String, String>     values = queryExhibitors(localExhibitors);

        int                     count = getCountFromValues(values);
        if ( count == 0 )
        {
            log.warn("0 count returned from Exhibitors. Using backup connection values.");
            values = useBackup(localExhibitors);
            count = getCountFromValues(values);
        }

        if ( count > 0 )
        {
            int                     port = Integer.parseInt(values.get(VALUE_PORT));
            StringBuilder           newConnectionString = new StringBuilder();
            List<String>            newHostnames = Lists.newArrayList();

            for ( int i = 0; i < count; ++i )
            {
                if ( newConnectionString.length() > 0 )
                {
                    newConnectionString.append(",");
                }
                String      server = values.get(VALUE_SERVER_PREFIX + i);
                newConnectionString.append(server).append(":").append(port);
                newHostnames.add(server);
            }

            String newConnectionStringValue = newConnectionString.toString();
            if ( !newConnectionStringValue.equals(connectionString.get()) )
            {
                log.info(String.format("Connection string has changed. Old value (%s), new value (%s)", connectionString.get(), newConnectionStringValue));
            }
            Exhibitors newExhibitors = new Exhibitors
            (
                newHostnames,
                localExhibitors.getRestPort(),
                new Exhibitors.BackupConnectionStringProvider()
                {
                    @Override
                    public String getBackupConnectionString() throws Exception
                    {
                        return masterExhibitors.get().getBackupConnectionString();  // this may be overloaded by clients. Make sure there is always a method call to get the value.
                    }
                }
            );
            connectionString.set(newConnectionStringValue);
            exhibitors.set(newExhibitors);
        }
    }

    private int getCountFromValues(Map<String, String> values)
    {
        try
        {
            return Integer.parseInt(values.get(VALUE_COUNT));
        }
        catch ( NumberFormatException e )
        {
            // ignore
        }
        return 0;
    }

    private Map<String, String> useBackup(Exhibitors localExhibitors)
    {
        Map<String, String>     values = newValues();

        try
        {
            String      backupConnectionString = localExhibitors.getBackupConnectionString();

            int         thePort = -1;
            int         count = 0;
            for ( String spec : backupConnectionString.split(",") )
            {
                spec = spec.trim();
                String[]        parts = spec.split(":");
                if ( parts.length == 2 )
                {
                    String      hostname = parts[0];
                    int         port = Integer.parseInt(parts[1]);
                    if ( thePort < 0 )
                    {
                        thePort = port;
                    }
                    else if ( port != thePort )
                    {
                        log.warn("Inconsistent port in connection component: " + spec);
                    }
                    values.put(VALUE_SERVER_PREFIX + count, hostname);
                    ++count;
                }
                else
                {
                    log.warn("Bad backup connection component: " + spec);
                }
            }
            values.put(VALUE_COUNT, Integer.toString(count));
            values.put(VALUE_PORT, Integer.toString(thePort));
        }
        catch ( Exception e )
        {
            log.error("Couldn't get backup connection string", e);
        }
        return values;
    }

    private Map<String, String> newValues()
    {
        Map<String, String>     values = Maps.newHashMap();
        values.put(VALUE_COUNT, "0");
        return values;
    }

    private static Map<String, String> decodeExhibitorList(String str) throws UnsupportedEncodingException
    {
        Map<String, String>     values = Maps.newHashMap();
        for ( String spec : str.split("&") )
        {
            String[]        parts = spec.split("=");
            if ( parts.length == 2 )
            {
                values.put(parts[0], URLDecoder.decode(parts[1], "UTF-8"));
            }
        }

        return values;
    }

    private Map<String, String> queryExhibitors(Exhibitors localExhibitors)
    {
        Map<String, String> values = newValues();

        long                    start = System.currentTimeMillis();
        int                     retries = 0;
        boolean                 done = false;
        while ( !done )
        {
            List<String> hostnames = Lists.newArrayList(localExhibitors.getHostnames());
            if ( hostnames.size() == 0 )
            {
                done = true;
            }
            else
            {
                String          hostname = hostnames.get(random.nextInt(hostnames.size()));
                try
                {
                    String                  encoded = restClient.getRaw(hostname, localExhibitors.getRestPort(), restUriPath, MIME_TYPE);
                    values.putAll(decodeExhibitorList(encoded));
                    done = true;
                }
                catch ( Throwable e )
                {
                    if ( retryPolicy.allowRetry(retries++, System.currentTimeMillis() - start, RetryLoop.getDefaultRetrySleeper()) )
                    {
                        log.warn("Couldn't get servers from Exhibitor. Retrying.", e);
                    }
                    else
                    {
                        log.error("Couldn't get servers from Exhibitor. Giving up.", e);
                        done = true;
                    }
                }
            }
        }

        return values;
    }
}
