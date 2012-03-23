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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.curator.RetryPolicy;
import com.netflix.curator.ensemble.EnsembleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
    private final ExhibitorRestClient restClient;
    private final String restUriPath;
    private final int pollingMs;
    private final RetryPolicy retryPolicy;
    private final ExecutorService service = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("ExhibitorEnsembleProvider-%d").build());
    private final Random random = new Random();
    private final AtomicReference<String> connectionString = new AtomicReference<String>("");
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);

    private static final String MIME_TYPE = "application/x-www-form-urlencoded";

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
    }

    /**
     * Can be called prior to starting the Curator instance to set the current connection string
     *
     * @throws Exception errors
     */
    public void     pollForInitialEnsemble() throws Exception
    {
        Preconditions.checkState(state.get() == State.LATENT);
        poll();
    }

    @Override
    public void start() throws Exception
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED));

        service.submit
        (
            new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        while ( !Thread.currentThread().isInterrupted() )
                        {
                            poll();
                            Thread.sleep(pollingMs);
                        }
                    }
                    catch ( InterruptedException e )
                    {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        );
    }

    @Override
    public void close() throws IOException
    {
        Preconditions.checkState(state.compareAndSet(State.STARTED, State.CLOSED));

        service.shutdownNow();
    }

    @Override
    public String getConnectionString()
    {
        String      localConnectionString = connectionString.get();
        if ( (localConnectionString == null) || (localConnectionString.length() == 0) )
        {
            localConnectionString = exhibitors.get().getBackupConnectionString();
        }
        return localConnectionString;
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

    private void poll()
    {
        long        start = System.currentTimeMillis();
        int         retries = 0;
        boolean     done = false;
        while ( !done )
        {
            Exhibitors      localExhibitors = exhibitors.get();
            
            List<String>    hostnames = Lists.newArrayList(localExhibitors.getHostnames());
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
                    Map<String, String>     values = decodeExhibitorList(encoded);

                    int                     port = Integer.parseInt(values.get("port"));
                    StringBuilder           newConnectionString = new StringBuilder();
                    List<String>            newHostnames = Lists.newArrayList();
                    for ( int i = 0; i < Integer.parseInt(values.get("count")); ++i )
                    {
                        if ( newConnectionString.length() > 0 )
                        {
                            newConnectionString.append(",");
                        }
                        String      server = values.get("server" + i);
                        newConnectionString.append(server).append(":").append(port);
                        newHostnames.add(server);
                    }

                    connectionString.set(newConnectionString.toString());
                    exhibitors.set(new Exhibitors(newHostnames, localExhibitors.getRestPort(), localExhibitors.getBackupConnectionString()));
                    done = true;
                }
                catch ( Throwable e )
                {
                    if ( retryPolicy.allowRetry(retries++, System.currentTimeMillis() - start) )
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
    }
}
