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

import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.util.JSON;

/**
 *
 *
 * @author Daniel Murygin <dm[at]sernet[dot]de>
 */
public class MongoPersistHandler implements IPersistHandler {

    private static final Logger LOG = Logger.getLogger(MongoPersistHandler.class);
    
    public static final String ITEM_COLLECTION_NAME = "item";
    
    private Configuration conf;
    
    private MongoClient mongoClient = null;
    private DB db = null;
    private DBCollection collection = null;
    
    
    public MongoPersistHandler(Configuration conf) {
        super();
        this.conf = conf;
        try {
            createMongoClient();
        } catch (UnknownHostException e) {
            LOG.error("Unknown host. Can not create MongoDB client.", e);
        }
    }
    
    private void createMongoClient() throws UnknownHostException {
        // init CouchbaseClient
        List<ServerAddress> adressList = new LinkedList<ServerAddress>();
        for (String url : conf.getDbUrls()) {
            adressList.add(new ServerAddress(url));
        }
        mongoClient = new MongoClient(adressList);    
    }

    /* (non-Javadoc)
     * @see org.wikidata.couchbase.IPersistHandler#save(java.lang.Integer, java.lang.String)
     */
    @Override
    public void save(Integer id, String json) {
        DBObject dbObject = (DBObject)JSON.parse(json);
        String idString = buildDocumentKey(id);
        dbObject.put("_id", idString);
        try {
            getCollection().insert(dbObject);
        } catch (MongoException.DuplicateKey e) {
            LOG.info("Item " + id + " exists and is updated");
            if (LOG.isDebugEnabled()) {
                LOG.debug("Stacktrace: ", e);
            }
            getCollection().save(dbObject);
        }
    }
    
    private String buildDocumentKey(Integer startId) {
        StringBuilder sb = new StringBuilder();
        sb.append("wikidata:item:");
        sb.append(startId);
        return sb.toString();
    }

    /* (non-Javadoc)
     * @see org.wikidata.couchbase.IPersistHandler#shutdown()
     */
    @Override
    public void shutdown() {
        mongoClient.close();
    }
    
    private DB getDb() {
        if(db==null) {
            db = mongoClient.getDB(conf.getDb());
        }
        return db;
    }
    
    private DBCollection getCollection() {  
        if(collection==null) {
            collection = getDb().getCollection(MongoPersistHandler.ITEM_COLLECTION_NAME);
        }
        return collection;
    }

}
