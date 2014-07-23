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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 *
 *
 * @author Daniel Murygin <dm[at]sernet[dot]de>
 */
public class ClaimProcessor implements IITemProcessor {

    private static final Logger LOG = Logger.getLogger(ClaimProcessor.class);
    
    PersistService persistService = null;
    
    String propertyName;
    
    String[] languages = {"en","es","de","fr","ru","zh","it","pt"};
    
    /**
     * @param propertyName
     */
    public ClaimProcessor(String propertyName) {
        super();
        this.propertyName = propertyName;
    }

    /* (non-Javadoc)
     * @see org.wikidata.couchbase.IITemProcessor#run(com.fasterxml.jackson.databind.JsonNode)
     */
    @Override
    public void run(JsonNode item) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing item: " + item.path("item").path("title").textValue());
        }
        Iterator<JsonNode> nodeIterator = item.get("item").path("claims").iterator();
        while(nodeIterator.hasNext()) {
            Iterator<JsonNode> claimIterator = nodeIterator.next().iterator();
            while(claimIterator.hasNext()) {
                JsonNode claim = claimIterator.next();
                String name = claim.path("mainsnak").path("property").textValue();
                if(propertyName.equals(name)) {
                    insertClaim(claim.path("mainsnak").path("datavalue"));
                }
                
            }
        }
    }

    /**
     * @param path
     */
    private void insertClaim(JsonNode claim) {
        long itemId = -1;
        if(claim!=null && claim.path("value")!=null && claim.path("value").path("numeric-id")!=null) {
            itemId = claim.path("value").path("numeric-id").asLong();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Inserting claim: " + itemId);
        }
        BasicDBObject doc = new BasicDBObject("property", propertyName)
        .append("itemid", itemId);
        doc.put("_id", createId(itemId));
        BasicDBObject query = new BasicDBObject("_id", MongoPersistHandler.buildDocumentKey((int) itemId));
        
        DBCursor cursor = getPersistService().find(query);
        if(cursor.hasNext()) {
            DBObject labels = (DBObject)((DBObject)cursor.next().get("item")).get("labels");
            List<Object> labelList = new ArrayList<Object>();
            for (String lan : languages) {
                if(labels.get(lan)!=null) {
                    labelList.add(labels.get(lan));
                }
            }
            doc.put("labels", labelList);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Labels added to claim: " + doc.get("labels"));
            }
        }
        
        getPersistService().save(doc);
    }

    /**
     * @param itemId
     * @return
     */
    private String createId(long itemId) {
        return new StringBuilder().append(propertyName).append("-").append(itemId).toString();
    }

    public PersistService getPersistService() {
        return persistService;
    }

    @Override
    public void setPersistService(PersistService persistService) {
        this.persistService = persistService;
    }

}
