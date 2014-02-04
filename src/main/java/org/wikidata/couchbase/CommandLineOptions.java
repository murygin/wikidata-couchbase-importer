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
    
    public static final String FIRST_ID = "f";
    public static final String LAST_ID = "l";
    public static final String NUMBER_OF_THREADS = "t";
    public static final String FIRST_ID_LONG = "first";
    public static final String LAST_ID_LONG = "last";
    public static final String NUMBER_OF_THREADS_LONG = "threads";
    
    public static Options get() {
        Options options = new Options();
        Option firstId = OptionBuilder.hasArg().withArgName(FIRST_ID_LONG).withDescription("First wikidata id (default: 0)").create(FIRST_ID);
        options.addOption(firstId);
        Option lastId = OptionBuilder.hasArg().withArgName(LAST_ID_LONG).withDescription("Last wikidata id (default: first + 10)").create(LAST_ID);
        options.addOption(lastId); 
        Option numberOfThreads = OptionBuilder.hasArg().withArgName(NUMBER_OF_THREADS_LONG).withDescription("Number of parallel threads (default 5)").create(NUMBER_OF_THREADS);
        options.addOption(numberOfThreads); 
        return options;
    }
}
