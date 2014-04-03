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

/**
 *
 *
 * @author Daniel Murygin <dm[at]sernet[dot]de>
 */
public class PersistService {

    private Configuration conf;
    
    private IPersistHandler handler;
    
    public PersistService(Configuration conf) {
        super();
        this.conf = conf;
        createHandler();
    }

    private void createHandler() {
        if(Configuration.DB_TYPE_COUCHBASE.equals(conf.getDbType())) { 
            handler = new CouchbasePersistHandler(conf);
        }
        if(Configuration.DB_TYPE_MONGO.equals(conf.getDbType())) { 
            handler = new MongoPersistHandler(conf);
        }      
    }

    public void save(Integer id, String json) {
        getHandler().save(id,json);
    }
    
    public void shutdown() {
        getHandler().shutdown();
    }

    public IPersistHandler getHandler() {
        return handler;
    }

    public void setHandler(IPersistHandler handler) {
        this.handler = handler;
    }
}
