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

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

/**
 *
 *
 * @author Daniel Murygin <dm[at]sernet[dot]de>
 */
public class CommandLineOptions {
    
    public static final String DB_TYPE = "t";
    public static final String DB_TYPE_LONG = "type";
    
    public static final String DB_URLS = "u";
    public static final String DB_URLS_LONG = "urls";
    
    public static final String DB = "d";
    public static final String DB_LONG = "db";
    
    public static final String FIRST_ID = "f";   
    public static final String FIRST_ID_LONG = "first";
    
    public static final String LAST_ID = "l";
    public static final String LAST_ID_LONG = "last";

    public static final String NUMBER_OF_THREADS = "n";
    public static final String NUMBER_OF_THREADS_LONG = "threads";
    
    public static final String HELP = "h";
    public static final String HELP_LONG = "help";
    
    @SuppressWarnings("static-access")
    public static Options get() {
        Options options = new Options();
        
        Option type = OptionBuilder.hasArg().withLongOpt(DB_TYPE_LONG).withDescription("Database type: 'couchbae' or 'mongo' (default: mongo)").create(DB_TYPE);
        options.addOption(type);
        
        Option couchbaseUrls = OptionBuilder
                .hasArg()
                .hasArgs()
                .withValueSeparator(',')
                .withLongOpt(DB_URLS_LONG)
                .withDescription("Database URL(s), separated by ',' (default: 'http://127.0.0.1:8091/pools' for Couchbase, 'localhost' for MongoDB)")
                .create(DB_URLS);
        options.addOption(couchbaseUrls); 
        
        Option bucket = OptionBuilder.hasArg().withLongOpt(DB_LONG).withDescription("Database / Bucket name (default: wikidata)").create(DB);
        options.addOption(bucket);
        
        Option firstId = OptionBuilder.hasArg().withLongOpt(FIRST_ID_LONG).withDescription("First wikidata item id (default: 1)").create(FIRST_ID);
        options.addOption(firstId);
        
        Option lastId = OptionBuilder.hasArg().withLongOpt(LAST_ID_LONG).withDescription("Last wikidata item id (default: first wikidata id)").create(LAST_ID);
        options.addOption(lastId); 
        
        Option numberOfThreads = OptionBuilder.hasArg().withLongOpt(NUMBER_OF_THREADS_LONG).withDescription("Number of parallel threads (default 5)").create(NUMBER_OF_THREADS);
        options.addOption(numberOfThreads);
        
        Option help = OptionBuilder.withLongOpt(HELP_LONG).withDescription("Show help").create(HELP);
        options.addOption(help);
        
        return options;
    }
}
