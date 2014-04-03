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

import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import com.couchbase.client.CouchbaseClient;

/**
 *
 *
 * @author Daniel Murygin <dm[at]sernet[dot]de>
 */
public class CouchbasePersistHandler implements IPersistHandler {

    private static final Logger LOG = Logger.getLogger(CouchbasePersistHandler.class);
    
    private Configuration conf;
    
    private CouchbaseClient couchbaseClient = null;
    
    
    public CouchbasePersistHandler(Configuration conf) {
        super();
        this.conf = conf;
        createCouchbaseClient();
    }

    private void createCouchbaseClient() {
        // init CouchbaseClient
        List<URI> uris = new LinkedList<URI>();
        for (String url : conf.getDbUrls()) {
            uris.add(URI.create(url));
        }
        try {
            couchbaseClient = new CouchbaseClient(uris, conf.getDb(), "");
        } catch (Exception e) {
            LOG.error("Error while connecting to couchbase", e);
        }
    }
    
    /* (non-Javadoc)
     * @see org.wikidata.couchbase.IPersistHandler#save(java.lang.Integer, java.lang.String)
     */
    @Override
    public void save(Integer id, String json) {
        String key = buildDocumentKey(id);            
        couchbaseClient.set(key, 0, json);
    }
    
    /* (non-Javadoc)
     * @see org.wikidata.couchbase.IPersistHandler#shutdown()
     */
    @Override
    public void shutdown() {
        couchbaseClient.shutdown();     
    }
    
    private String buildDocumentKey(Integer startId) {
        StringBuilder sb = new StringBuilder();
        sb.append("wikidata:item:");
        sb.append(startId);
        return sb.toString();
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

}
