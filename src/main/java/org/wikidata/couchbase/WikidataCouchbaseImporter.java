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

import com.sun.jersey.api.client.Client;

/**
 * WikidataImporter loads items from Wikidata
 * and saves these items in a database (MongoDB or Couchbase).
 * 
 * Loading ans saving is done concurrently by multiple {@link WikidataImportThread}s.
 * 
 * Wikidata is a free knowledge base that can be read and edited by humans and machines alike. 
 * http://wikidata.org
 * 
 * Couchbase - http://www.couchbase.com/
 * MongoDB - http://www.mongodb.org/
 * 
 * @author Daniel Murygin <dm[at]sernet[dot]de>
 */
public class WikidataCouchbaseImporter {

    private static final Logger LOG = Logger.getLogger(WikidataCouchbaseImporter.class);
 
    private static final Integer MAX_NUMBER_PER_ITERATION_DEFAULT = 2000;
    private static long TERMINATION_TIMEOUT_IN_MINUTES = 15;
    
    // Text for command line help output
    private static final String USAGE = "java -jar wci.jar [-t <db_type>] [-u <db_url>] [-b <bucket>] [-f <first_id>] [-l <last_id>]";
    private static final String HEADER = "Wikidata importer (WCI), Copyright (c) 2014 Daniel Murygin.";
    private static final String FOOTER = "For more instructions, see: http://murygin.wordpress.com/2014/02/22/wikidata-couchbase-importer/";

    Configuration conf;
    
    private Integer startId;
    private Integer stopId;
    private int maxNumberPerIteration = MAX_NUMBER_PER_ITERATION_DEFAULT;
    
    private static ExecutorService taskExecutor;
    private static Client jerseyClient = null;
    
    private PersistService persistService = null;
    
    private long startTimestamp;

    /**
     * Creates a WikidataCouchbaseImporter with default parameters.
     */
    public WikidataCouchbaseImporter() {
        super(); 
        conf = Configuration.createDefault();
        init();
    }
    
    /**
     * Creates a WikidataCouchbaseImporter with given configuration.
     * 
     * @param conf Importer configuration
     */
    public WikidataCouchbaseImporter(Configuration conf) {
       super();
       this.conf = conf;
       init();
    }
    
    public static void main(String[] args) {       
        CommandLineParser parser = new GnuParser();
        Options options = CommandLineOptions.get();
        Configuration conf = null;
        WikidataCouchbaseImporter importer = null;
        try {           
            CommandLine cmd = parser.parse(options, args);
            conf = Configuration.createFromCommandLine(cmd);          
            boolean printHelp = cmd.hasOption(CommandLineOptions.HELP); 
            if(printHelp) {
                printUsage(options);
            } else {
                importer = new WikidataCouchbaseImporter(conf);
                importer.run();   
            }
        } catch (ParseException e) {
            LOG.error(e);
            printUsage(options);
        } catch (NumberFormatException e) {
            LOG.error(e);
            printUsage(options);
        } catch (InterruptedException e) {
            LOG.error(e);
            printUsage(options);
        } finally {
            if(importer!=null) {
                importer.shutdown();
            }
        }
    }
    
    private void init() {
        startTimestamp = initRuntime();
        
        // init Jersey client
        jerseyClient = Client.create();
        
        persistService = new PersistService(conf);

        // init thread executer
        taskExecutor = Executors.newFixedThreadPool(conf.getMaxNumberOfThreads());
    }

    /**
     * Run the import
     * @throws InterruptedException 
     */
    public void run() throws InterruptedException {
        LOG.info("Starting import...");
        logParameter();
        setStartId(conf.getFirstId());
        setStopId(0);
        while(getStopId() < conf.getLastId()) {
            if((conf.getLastId()-getStartId()) < maxNumberPerIteration) {
                setStopId(conf.getLastId());
            } else {
                setStopId(getStartId()+maxNumberPerIteration);
            }
            LOG.info("Importing item " + getStartId() + " to " + getStopId() + "...");
            importItem(getStartId());
            taskExecutor.shutdown();
            taskExecutor.awaitTermination(TERMINATION_TIMEOUT_IN_MINUTES, TimeUnit.MINUTES);
            taskExecutor = Executors.newFixedThreadPool(conf.getMaxNumberOfThreads());
            setStartId(getStopId()+1);
        }
    }
    
    private void shutdown() {
        try {
            if(taskExecutor!=null) {
                taskExecutor.shutdown();
                taskExecutor.awaitTermination(TERMINATION_TIMEOUT_IN_MINUTES, TimeUnit.MINUTES);
                persistService.shutdown();
                logStatistics(startTimestamp, conf.getFirstId(), conf.getLastId());
            }             
        } catch (Exception e) {
            LOG.error("Error while shutting down.", e);
        } 
    }

    /**
     * Loads an item from Wikidata and saves it in a database.
     * 
     * Loading ans saving is done concurrently by multiple
     * {@link WikidataImportThread}s.
     * 
     * @param id The id of a wikidata item
     */
    private void importItem(int id) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Importing item " + id);
        }

        WikidataImportThread exportThread = new WikidataImportThread(jerseyClient, persistService, id);
        
        taskExecutor.execute(exportThread);
        int next = id + 1;
        if(next <= getStopId()) {
            importItem(next);
        }
    }
    
    public Integer getStartId() {
        return startId;
    }

    public void setStartId(Integer startId) {
        this.startId = startId;
    }

    public Integer getStopId() {
        return stopId;
    }

    public void setStopId(Integer stopId) {
        this.stopId = stopId;
    }

    private static void printUsage(Options options) {
        HelpFormatter helpFormatter = new HelpFormatter( );
        helpFormatter.setWidth( 80 );
        helpFormatter.printHelp( USAGE, HEADER, options, FOOTER );
    }

    private static long initRuntime() {
        return System.currentTimeMillis();
    }
    
    private void logParameter() {
        LOG.info("Database type: " + conf.getDbType());
        boolean first = true;
        StringBuilder sb = new StringBuilder();
        for (String url : conf.getDbUrls()) {
           if(!first) {
               sb.append(", ");              
           }
           first = false;
           sb.append(url);
        }
        
        LOG.info("Server urls: " + sb.toString());
        LOG.info("Database / bucket: " + conf.getDb());
        LOG.info("Number of threads: " + conf.getMaxNumberOfThreads());
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
