/*******************************************************************************
 * Copyright (c) 2013 Daniel Murygin.
 *
 * This program is free software: you can redistribute it and/or 
 * modify it under the terms of the GNU Lesser General Public License 
 * as published by the Free Software Foundation, either version 3 
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,    
 * but WITHOUT ANY WARRANTY; without even the implied warranty 
 * of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  
 * See the GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program. 
 * If not, see <http://www.gnu.org/licenses/>.
 * 
 * Contributors:
 *     Daniel Murygin <daniel.murygin[at]gmail[dot]com>
 ******************************************************************************/
package org.wikidata.couchbase;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.couchbase.client.CouchbaseClient;
import com.sun.jersey.api.client.Client;

/**
 * WikidataCouchbaseImporter loads items from Wikidata
 * and saves these items in couchbase.
 * 
 * Loading ans saving is done concurrently by multiple {@link WikidataImportThread}s.
 * 
 * Wikidata is a free knowledge base that can be read and edited by humans and machines alike. 
 * http://wikidata.org
 * 
 * Couchbase is a open source NoSQL database - http://www.couchbase.com/
 * 
 * @author Daniel Murygin <dm[at]sernet[dot]de>
 */
public class WikidataCouchbaseImporter {

    private static final Logger LOG = Logger.getLogger(WikidataCouchbaseImporter.class);
    
    // Default parameter values
    private static final String[] COUCHBASE_URLS_DEFAULT = {"http://127.0.0.1:8091/pools"};
    private static final String BUCKET_DEFAULT = "wikidata";
    private static final Integer FIRST_ID_DEFAULT = 1;
    private static final Integer MAX_NUMBER_Of_THREADS_DEFAULT = 5;
    private static long TERMINATION_TIMEOUT_IN_MINUTES = 15;
    
    // Text for command line help output
    private static final String USAGE = "java -jar wci.jar [-u <couchbase_url>] [-b <bucket>] [-f <first_id>] [-l <last_id>]";
    private static final String HEADER = "Wikidata couchbase importer, Copyright (c) 2014 Daniel Murygin.";
    private static final String FOOTER = "For more instructions, see: http://murygin.wordpress.com";
    
    private Integer firstId = FIRST_ID_DEFAULT;
    private Integer lastId;
    private static int maxNumberOfThreads = MAX_NUMBER_Of_THREADS_DEFAULT;
    private String[] couchbaseUrls = COUCHBASE_URLS_DEFAULT;
    private String bucket = BUCKET_DEFAULT;
    
    private static ExecutorService taskExecutor;
    private static Client jerseyClient = null;
    private static CouchbaseClient cb = null;

    /**
     * Creates a WikidataCouchbaseImporter with default parameters.
     */
    public WikidataCouchbaseImporter() {
        super(); 
        couchbaseUrls = COUCHBASE_URLS_DEFAULT;
        bucket = BUCKET_DEFAULT;
        firstId = FIRST_ID_DEFAULT;
        lastId = firstId;
        init();
    }
    
    /**
     * Creates a WikidataCouchbaseImporter with given parameter.
     * If one of the parameters is <code>null</code> the default value
     * is used for this parameter.
     * 
     * @param couchbaseUrlsParam An array with couchbase connection Urls, e.g.: http://127.0.0.1:8091/pools
     * @param bucketParam The name of a couchbase bucket
     * @param firstIdParam First wikidata id for import
     * @param lastIdParam Last wikidata id for import
     */
    public WikidataCouchbaseImporter(String[] couchbaseUrlsParam, String bucketParam, Integer firstIdParam, Integer lastIdParam) {
       super();
       couchbaseUrls = couchbaseUrlsParam;
       if(couchbaseUrls==null || couchbaseUrls.length<1) {
           couchbaseUrls = COUCHBASE_URLS_DEFAULT;
       }
       bucket = bucketParam;
       if(bucket==null) {
           bucket = BUCKET_DEFAULT;
       }
       this.firstId = firstIdParam;
       if(firstId==null) {
           firstId = FIRST_ID_DEFAULT;
       }
       this.lastId = lastIdParam;
       if(lastId==null) {
           lastId = firstId;
       }
       init();
    }
    
    private void init() {
        // init Jersey client
        jerseyClient = Client.create();

        // init CouchbaseClient
        List<URI> uris = new LinkedList<URI>();
        for (String url : couchbaseUrls) {
            uris.add(URI.create(url));
        }
       
        try {
            cb = new CouchbaseClient(uris, bucket, "");
        } catch (Exception e) {
            LOG.error("Error while connecting to couchbase", e);
        }

        // init thread executer
        taskExecutor = Executors.newFixedThreadPool(maxNumberOfThreads);
    }

    public static void main(String[] args) {
        long startTimestamp = initRuntime();
        CommandLineParser parser = new GnuParser();
        Options options = CommandLineOptions.get();
        Integer firstId = null;
        Integer lastId = null;
        try {           
            CommandLine cmd = parser.parse( options, args);
            firstId = Integer.valueOf(cmd.getOptionValue(CommandLineOptions.FIRST_ID, String.valueOf(FIRST_ID_DEFAULT)));
            lastId = Integer.valueOf(cmd.getOptionValue(CommandLineOptions.LAST_ID, String.valueOf(firstId)));
            maxNumberOfThreads = Integer.valueOf(cmd.getOptionValue(CommandLineOptions.NUMBER_OF_THREADS, String.valueOf(MAX_NUMBER_Of_THREADS_DEFAULT)));
            String[] urlsFromCmd = cmd.getOptionValues(CommandLineOptions.COUCHBASE_URLS); 
            String bucket = cmd.getOptionValue(CommandLineOptions.BUCKET, BUCKET_DEFAULT); 
            WikidataCouchbaseImporter importer = new WikidataCouchbaseImporter(urlsFromCmd,bucket,firstId,lastId);
            importer.run();          
        } catch (ParseException e) {
            LOG.error(e);
            printUsage(options);
        } catch (NumberFormatException e) {
            LOG.error(e);
            printUsage(options);
        } finally {
            try {
                if(taskExecutor!=null) {
                    taskExecutor.shutdown();
                }
                taskExecutor.awaitTermination(TERMINATION_TIMEOUT_IN_MINUTES, TimeUnit.MINUTES);
                cb.shutdown();
                logStatistics(startTimestamp, firstId, lastId);
            } catch (Exception e) {
                LOG.error("Error while shutting down.", e);
            }  
                      
        }
    }
    
    
    /**
     * Run the import
     */
    public void run() {
        LOG.info("Starting import...");
        logCouchbaseParameter();
        importItem(getFirstId());
    }

    /**
     * Loads an item from Wikidata and saves it in a Couchbase bucket.
     * 
     * Loading ans saving is done concurrently by multiple
     * {@link WikidataImportThread}s.
     * 
     * @param artistName
     *            The name of an Artist
     */
    private void importItem(int id) {
        if (LOG.isInfoEnabled()) {
            LOG.info("Importing item " + id);
        }

        WikidataImportThread exportThread = new WikidataImportThread(jerseyClient, cb, id);
        taskExecutor.execute(exportThread);
        int next = id + 1;
        if(next <= getLastId()) {
            importItem(next);
        }
    }
    
    public Integer getFirstId() {
        return firstId;
    }

    public void setFirstId(Integer startId) {
        this.firstId = startId;
    }

    public Integer getLastId() {
        return lastId;
    }

    public void setLastId(Integer endId) {
        this.lastId = endId;
    }

    public int getMaxNumberOfThreads() {
        return maxNumberOfThreads;
    }

    public void setMaxNumberOfThreads(int maxNumberOfThreads) {
        WikidataCouchbaseImporter.maxNumberOfThreads = maxNumberOfThreads;
    }
    
    private static void printUsage(Options options) {
        HelpFormatter helpFormatter = new HelpFormatter( );
        helpFormatter.setWidth( 80 );
        helpFormatter.printHelp( USAGE, HEADER, options, FOOTER );
    }

    private static long initRuntime() {
        return System.currentTimeMillis();
    }
    
    private void logCouchbaseParameter() {
        boolean first = true;
        StringBuilder sb = new StringBuilder();
        for (String url : couchbaseUrls) {
           if(!first) {
               sb.append(", ");              
           }
           first = false;
           sb.append(url);
        }
        LOG.info("Couchbase server urls: " + sb.toString());
        LOG.info("Couchbase bucket: " + bucket);
    }
    

    private static void logStatistics(long startTimestamp, int startId, int endId) {
       int n = endId - startId + 1;
       long runtimeInMs = System.currentTimeMillis()-startTimestamp;
       LOG.info("Import finished. " + n + " items imported.");
       logRuntime("Runtime: ", runtimeInMs);
       double itemsPerSecond = n / (runtimeInMs / 1000.0);
       LOG.info("Items per second: " + itemsPerSecond);
    }

    private static void logRuntime(String message, long runtimeInMs) {
        LOG.info(message + TimeFormatter.getHumanRedableTime(runtimeInMs));
    }

}
