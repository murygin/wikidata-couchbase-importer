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

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.DBObject;

/**
 *
 *
 * @author Daniel Murygin <dm[at]sernet[dot]de>
 */
public class WikidataIteratorThread extends Thread {

    private static final Logger LOG = Logger.getLogger(WikidataImportThread.class);
    
    PersistService persistService = null;
    ObjectMapper mapper = new ObjectMapper();
    Integer start;
    Integer limit;
    
    List<IITemProcessor> itemProcessors;
    
    public WikidataIteratorThread(PersistService persistService, Integer start, Integer limit) {
        super();
        itemProcessors = new LinkedList<IITemProcessor>();
        this.persistService = persistService;
        this.start = start;
        this.limit = limit;
    }

    /* (non-Javadoc)
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
        for (IITemProcessor processor : itemProcessors) {
            processor.setPersistService(persistService);
        }
        List<DBObject> itemList = persistService.load(start, limit);
        for (DBObject dbObject : itemList) {
            try {
                processItem(createNode(dbObject));
            } catch (Exception e) {
                LOG.error("Error while processing db-object: " + dbObject, e);
            }
        }
    }
    
    private void processItem(JsonNode item) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing item: " + item.path("item").path("title").textValue());
        }
        for (IITemProcessor processor : itemProcessors) {
            processor.run(item);
        }
    }
    
    protected List<JsonNode> createNodeList(List<DBObject> result) throws IOException, JsonProcessingException {
        List<JsonNode> jsonNodeList = new ArrayList<JsonNode>(result.size());
        for (DBObject dbObject : result) {
            jsonNodeList.add(createNode(dbObject));
        }
        return jsonNodeList;
    }
    
    protected JsonNode createNode(DBObject dbObject) throws JsonProcessingException, IOException {
        return mapper.readTree(dbObject.toString());
    }
    
    public void addProcessor(IITemProcessor processor) {
        itemProcessors.add(processor);
    }
}
