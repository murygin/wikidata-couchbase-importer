/*******************************************************************************
 * Copyright (c) 2014 Daniel Murygin.
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
 *     Daniel Murygin <dm[at]sernet[dot]de> - initial API and implementation
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
import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;

/**
 *
 *
 * @author Daniel Murygin <dm[at]sernet[dot]de>
 */
public class WikidataIterator {

    private static final Logger LOG = Logger.getLogger(WikidataIterator.class);
    
    private static long TERMINATION_TIMEOUT_IN_MINUTES = 15;
    
    // Text for command line help output
    private static final String USAGE = "java -jar wci.jar [-t <db_type>] [-u <db_url>] [-b <bucket>] [-f <first_id>] [-l <last_id>]";
    private static final String HEADER = "Wikidata iterator, Copyright (c) 2014 Daniel Murygin.";
    private static final String FOOTER = "For more instructions, see: http://murygin.wordpress.com/2014/02/22/wikidata-couchbase-importer/";

    private String property = "P31";

    private int numberPerThread = 10;
    private long startTimestamp;
    
    private Configuration conf;
    
    private Integer startId;
    private Integer stopId;
    
    private static ExecutorService taskExecutor;
    
    private PersistService persistService = null;
      
    /**
     * 
     */
    public WikidataIterator(Configuration conf) {
        super();
        this.conf = conf;
        init();
    }

    private void init() {
        startTimestamp = initRuntime();

        // init thread executer
        taskExecutor = Executors.newFixedThreadPool(conf.getMaxNumberOfThreads());
        
        persistService = new PersistService(conf);
    }
    
    private void run() throws InterruptedException {
        LOG.info("Start iterating...");      
        if(conf.getLastId()==null) {
            conf.setLastId((int)persistService.count());
        }
        logParameter();
        setStartId(conf.getFirstId());
        setStopId(0);
        while(getStopId() < conf.getLastId()) {
            if((conf.getLastId()-getStartId()) < numberPerThread) {
                setStopId(conf.getLastId());
            } else {
                setStopId(getStartId()+numberPerThread);
            }
            LOG.info("Importing item " + getStartId() + " to " + getStopId() + "...");
            WikidataIteratorThread thread = new WikidataIteratorThread(persistService, getStartId(),numberPerThread);
            thread.addProcessor(new ClaimProcessor(property));
            taskExecutor.execute(thread);
            setStartId(getStopId()+1);
        }
    }

   

    private void shutdown() {
        try {
            if(taskExecutor!=null) {
                taskExecutor.shutdown();
                taskExecutor.awaitTermination(TERMINATION_TIMEOUT_IN_MINUTES, TimeUnit.MINUTES);
                logStatistics(startTimestamp,conf.getFirstId(), conf.getLastId());
                logDbStatus();
            }             
        } catch (Exception e) {
            LOG.error("Error while shutting down.", e);
        } 
        
    }

    /**
     * 
     */
    private void logDbStatus() {
        BasicDBObject doc = new BasicDBObject("property", property);
        DBCursor cursor = persistService.find(doc);
        LOG.info("Number of " + property + " properties in DB: " + cursor.count());
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        CommandLineParser parser = new GnuParser();
        Options options = CommandLineOptions.get();
        WikidataIterator iterator = null;
        Configuration conf = null;
        try {           
            CommandLine cmd = parser.parse(options, args);
            conf = Configuration.createForIteratorFromCommandLine(cmd);          
            boolean printHelp = cmd.hasOption(CommandLineOptions.HELP); 
            if(printHelp) {
                printUsage(options);
            } else {
                iterator = new WikidataIterator(conf);
                iterator.run();   
            }  
            
        } catch (Exception e) {
            LOG.error(e);
        } finally {
            if(iterator!=null) {
                iterator.shutdown();
            }
        }

    }

    private static long initRuntime() {
        return System.currentTimeMillis();
    }
    
    private static void printUsage(Options options) {
        HelpFormatter helpFormatter = new HelpFormatter( );
        helpFormatter.setWidth( 80 );
        helpFormatter.printHelp( USAGE, HEADER, options, FOOTER );
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
