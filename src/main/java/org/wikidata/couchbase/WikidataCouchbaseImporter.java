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
 * Loading ans saving  is done concurrently by multiple {@link WikidataImportThread}s.
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

    private static final String USAGE = "[-f <first_id>] [-l <last_id>]";
    private static final String HEADER = "Wikidata couchbase importer, Copyright (c) 2014 Daniel Murygin.";
    private static final String FOOTER = "For more instructions, see: http://murygin.wordpress.com";
    
    private Integer startId;
    private Integer endId;
    private static int maxNumberOfThreads = 5;
    
    private static long timeoutInMinutes = 15;

    private static ExecutorService taskExecutor;
    private static Client jerseyClient = null;
    private static CouchbaseClient cb = null;

    public WikidataCouchbaseImporter() {
        super();

        // init Jersey client
        jerseyClient = Client.create();

        // init CouchbaseClient
        List<URI> uris = new LinkedList<URI>();
        uris.add(URI.create("http://127.0.0.1:8091/pools"));
        try {
            cb = new CouchbaseClient(uris, "wikidata", "");
        } catch (Exception e) {
            System.err.println("Error connecting to Couchbase: " + e.getMessage());
        }

        // init thread executer
        taskExecutor = Executors.newFixedThreadPool(maxNumberOfThreads);
    }

    /**
     * @param defaultStartId
     * @param defaultEndId
     */
    public WikidataCouchbaseImporter(Integer startId, Integer endId) {
       this();
       this.startId = startId;
       this.endId = endId;   
    }

    public static void main(String[] args) {
        long startTimestamp = initRuntime();
        CommandLineParser parser = new GnuParser();
        Options options = CommandLineOptions.get();
        Integer firstId = 0;
        Integer lastId = 0;
        try {           
            CommandLine cmd = parser.parse( options, args);
            firstId = Integer.valueOf(cmd.getOptionValue(CommandLineOptions.FIRST_ID, "0"));
            lastId = Integer.valueOf(cmd.getOptionValue(CommandLineOptions.LAST_ID, String.valueOf(firstId + 10)));
            maxNumberOfThreads = Integer.valueOf(cmd.getOptionValue(CommandLineOptions.NUMBER_OF_THREADS, String.valueOf(5)));
            WikidataCouchbaseImporter importer = new WikidataCouchbaseImporter(firstId,lastId);
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
                taskExecutor.awaitTermination(timeoutInMinutes, TimeUnit.MINUTES);
                cb.shutdown();
                logStatistics(startTimestamp, firstId, lastId);
            } catch (Exception e) {
                LOG.error("Error while shutting down.", e);
            }  
                      
        }
    }
    
    private void run() {     
        importItem(getStartId());
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
            LOG.info("");
            LOG.info("");
            LOG.info("Importing item " + id);
        }

        WikidataImportThread exportThread = new WikidataImportThread(jerseyClient, cb, id);
        taskExecutor.execute(exportThread);
        int next = id + 1;
        if(next <= getEndId()) {
            importItem(next);
        }
    }
    
    public Integer getStartId() {
        return startId;
    }

    public void setStartId(Integer startId) {
        this.startId = startId;
    }

    public Integer getEndId() {
        return endId;
    }

    public void setEndId(Integer endId) {
        this.endId = endId;
    }

    public int getMaxNumberOfThreads() {
        return maxNumberOfThreads;
    }

    public void setMaxNumberOfThreads(int maxNumberOfThreads) {
        this.maxNumberOfThreads = maxNumberOfThreads;
    }
    
    private static void printUsage(Options options) {
        HelpFormatter helpFormatter = new HelpFormatter( );
        helpFormatter.setWidth( 80 );
        helpFormatter.printHelp( USAGE, HEADER, options, FOOTER );
    }

    private static long initRuntime() {
        return System.currentTimeMillis();
    }
    

    private static void logStatistics(long startTimestamp, int startId, int endId) {
       int n = endId - startId;
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
