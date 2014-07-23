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

import org.apache.commons.cli.CommandLine;

/**
 *
 *
 * @author Daniel Murygin <dm[at]sernet[dot]de>
 */
public class Configuration {
    
    public static final String DB_TYPE_COUCHBASE = "couchbase";
    public static final String DB_TYPE_MONGO = "mongo";
    
    // Default parameter values
    public static final String[] DB_URLS_DEFAULT_COUCHBASE = {"http://127.0.0.1:8091/pools"};
    public static final String[] DB_URLS_DEFAULT_MONGO = {"localhost"};
    public static final String DB_DEFAULT = "wikidata";
    public static final Integer FIRST_ID_DEFAULT = 1;
    public static final Integer MAX_NUMBER_Of_THREADS_DEFAULT = 5;
    public static final String DB_TYPE_DEFAULT = DB_TYPE_MONGO;  
    
    private String[] dbUrls = DB_URLS_DEFAULT_COUCHBASE;
    private String db = DB_DEFAULT;
    private Integer firstId = FIRST_ID_DEFAULT;
    private Integer lastId;
    private int maxNumberOfThreads = MAX_NUMBER_Of_THREADS_DEFAULT;
    
    private String dbType = DB_TYPE_DEFAULT;
    
    public static Configuration createDefault() {  
        Configuration conf = new Configuration();
        conf.setDb(DB_DEFAULT);
        conf.setDbUrls(DB_URLS_DEFAULT_COUCHBASE);
        conf.setFirstId(FIRST_ID_DEFAULT);
        conf.setLastId(FIRST_ID_DEFAULT);
        conf.setMaxNumberOfThreads(MAX_NUMBER_Of_THREADS_DEFAULT);
        conf.setDbType(DB_TYPE_DEFAULT);
        return conf;
    }
    
    public static Configuration createFromCommandLine(CommandLine cmd) {
        String dbType = cmd.getOptionValue(CommandLineOptions.DB_TYPE, DB_TYPE_MONGO);
        Integer firstIdParam = Integer.valueOf(cmd.getOptionValue(CommandLineOptions.FIRST_ID, String.valueOf(FIRST_ID_DEFAULT)));
        Integer lastIdParam = Integer.valueOf(cmd.getOptionValue(CommandLineOptions.LAST_ID, String.valueOf(firstIdParam)));
        int maxNumberOfThreadsParam = Integer.valueOf(cmd.getOptionValue(CommandLineOptions.NUMBER_OF_THREADS, String.valueOf(MAX_NUMBER_Of_THREADS_DEFAULT)));
        String[] urlsFromCmd = cmd.getOptionValues(CommandLineOptions.DB_URLS); 
        String db = cmd.getOptionValue(CommandLineOptions.DB, DB_DEFAULT); 
        
        if(!DB_TYPE_COUCHBASE.equals(dbType) && !DB_TYPE_MONGO.equals(dbType)) {
            dbType = DB_TYPE_MONGO;
        }
        
        if(urlsFromCmd==null || urlsFromCmd.length<1) {
            urlsFromCmd = (DB_TYPE_COUCHBASE.equals(dbType)) ? DB_URLS_DEFAULT_COUCHBASE : DB_URLS_DEFAULT_MONGO;
        }
        if(db==null) {
            db = DB_DEFAULT;
        }
        if(firstIdParam==null) {
            firstIdParam = FIRST_ID_DEFAULT;
        }
        if(lastIdParam==null) {
            lastIdParam = firstIdParam;
        }
        
        
        Configuration conf = new Configuration();
        conf.setDbType(dbType);
        conf.setDb(db);
        conf.setDbUrls(urlsFromCmd);
        conf.setFirstId(firstIdParam);
        conf.setLastId(lastIdParam);
        conf.setMaxNumberOfThreads(maxNumberOfThreadsParam);
        return conf;
    }
    
    public static Configuration createForIteratorFromCommandLine(CommandLine cmd) {
        String dbType = cmd.getOptionValue(CommandLineOptions.DB_TYPE, DB_TYPE_MONGO);
        Integer firstIdParam = Integer.valueOf(cmd.getOptionValue(CommandLineOptions.FIRST_ID, String.valueOf(FIRST_ID_DEFAULT)));
        Integer lastIdParam = null;
        if(cmd.getOptionValue(CommandLineOptions.LAST_ID)!=null) {
                lastIdParam = Integer.valueOf(cmd.getOptionValue(CommandLineOptions.LAST_ID));
        }
        int maxNumberOfThreadsParam = Integer.valueOf(cmd.getOptionValue(CommandLineOptions.NUMBER_OF_THREADS, String.valueOf(MAX_NUMBER_Of_THREADS_DEFAULT)));
        String[] urlsFromCmd = cmd.getOptionValues(CommandLineOptions.DB_URLS); 
        String db = cmd.getOptionValue(CommandLineOptions.DB, DB_DEFAULT); 
        
        if(!DB_TYPE_COUCHBASE.equals(dbType) && !DB_TYPE_MONGO.equals(dbType)) {
            dbType = DB_TYPE_MONGO;
        }
        
        if(urlsFromCmd==null || urlsFromCmd.length<1) {
            urlsFromCmd = (DB_TYPE_COUCHBASE.equals(dbType)) ? DB_URLS_DEFAULT_COUCHBASE : DB_URLS_DEFAULT_MONGO;
        }
        if(db==null) {
            db = DB_DEFAULT;
        }
        if(firstIdParam==null) {
            firstIdParam = FIRST_ID_DEFAULT;
        }
    
        Configuration conf = new Configuration();
        conf.setDbType(dbType);
        conf.setDb(db);
        conf.setDbUrls(urlsFromCmd);
        conf.setFirstId(firstIdParam);
        conf.setLastId(lastIdParam);
        conf.setMaxNumberOfThreads(maxNumberOfThreadsParam);
        return conf;
    }
    
    public Configuration() {
        super();
    }
    
    public String[] getDbUrls() {
        return dbUrls;
    }
    public void setDbUrls(String[] dbUrls) {
        this.dbUrls = dbUrls;
    }
    public String getDb() {
        return db;
    }
    public void setDb(String db) {
        this.db = db;
    }
    public Integer getFirstId() {
        return firstId;
    }
    public void setFirstId(Integer firstId) {
        this.firstId = firstId;
    }
    public Integer getLastId() {
        return lastId;
    }
    public void setLastId(Integer lastId) {
        this.lastId = lastId;
    }
    public int getMaxNumberOfThreads() {
        return maxNumberOfThreads;
    }
    public void setMaxNumberOfThreads(int maxNumberOfThreads) {
        this.maxNumberOfThreads = maxNumberOfThreads;
    }

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String persistType) {
        this.dbType = persistType;
    }
    
    
}
