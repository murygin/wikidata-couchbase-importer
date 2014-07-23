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
 *     Daniel Murygin <daniel.murygin[at]gmail[dot]com>
 ******************************************************************************/
package org.wikidata.couchbase;

import org.apache.log4j.Logger;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;

/**
 * This thread loads information about an item from wikidata
 * ans saves JSON response in a database.
 * 
 * {@link WikidataCouchbaseImporter} uses multiple WikidataImportThread
 * to import data from wikidata
 * 
 * wikidata API - http://www.wikidata.org/wiki/Wikidata:Data_access
 *
 * @author Daniel Murygin <dm[at]sernet[dot]de>
 */
public class WikidataImportThread extends Thread {
    
    private static final Logger LOG = Logger.getLogger(WikidataImportThread.class);
    
    Client jerseyClient = null;
    PersistService persistService = null;
    Integer startId;
    
    public WikidataImportThread(Client jerseyClient, PersistService persistService, Integer startId) {
        super();
        this.jerseyClient = jerseyClient;
        this.persistService = persistService;
        this.startId = startId;
    }
    
    /* (non-Javadoc)
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
        exportItem(startId);
    }
    
    private void exportItem(Integer startId) {                
        if (LOG.isDebugEnabled()) {
            LOG.debug("Loading item with id: " + startId + " from wikidata...");
        }
        try {
            String jsonResponse = getItemAsJson(startId);         
            if (LOG.isDebugEnabled()) {
                LOG.debug("Wikidata JSON response: " + jsonResponse);
            } 
            saveJsonInDatabase(startId, jsonResponse);   
            if (LOG.isInfoEnabled()) {
                LOG.info("Item " + startId + " saved in db.");
            }
        } catch (UniformInterfaceException uie) {
            int status = -1;
            if(uie!=null && uie.getResponse()!=null) {
                status = uie.getResponse().getStatus();
            }
            LOG.warn("Item " + startId + " was not exported. HTTP error: " + status);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Stacktrace: ", uie);
            }
        } catch (com.sun.jersey.api.client.ClientHandlerException che) {     
            LOG.warn("Item " + startId + " was not exported. Unknow error, maybe a network problem: " + che.getMessage());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Stacktrace: ", che);
            }
        }
        
        catch (Exception e) {
            LOG.warn("Item " + startId + " was not exported. Unknow error: " + e.getMessage());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Stacktrace: ", e);
            }
        }
    }
    
    private String getItemAsJson(Integer id) {
        String url = buildWebserviceUrl(id);
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("Webservice URL: " + url);
        }
        
        WebResource webResource = jerseyClient.resource(url);
        String jsonResponse = webResource.get(String.class);
        return jsonResponse;
    }

    private void saveJsonInDatabase(Integer id, String jsonResponse) {
        persistService.save(id, jsonResponse);
        
    }
    
    private String buildWebserviceUrl(Integer id) {
        StringBuilder sb = new StringBuilder();
        sb.append("https://www.wikidata.org/wiki/Special:EntityData/Q");
        sb.append(id);
        sb.append(".json");    
        return sb.toString();
    }

}
