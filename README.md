Wikidata-Couchbase-Importer (WCI)
=================================


Imports data from wikidata. Saves data in Couchbase Server.



Build & Run WCI
---------------

* name@servant:~$ git clone https://github.com/murygin/wikidata-couchbase-importer.git
* name@servant:~$ cd wikidata-couchbase-importer
* name@servant:~$ mvn package
* name@servant:~$ cd target
* name@servant:~$ java -jar wci.jar [-u couchbase_url_1[,couchbase_url_2]] [-b bucket] [-f first_id] [-l last_id]



Wikidata
--------

Wikidata is a free knowledge base that can be read and edited by humans and machines alike. 
It is for data what Wikimedia Commons is for media files: 
it centralizes access to and management of structured data, 
such as interwiki references and statistical information. 
Wikidata contains data in every language supported by the MediaWiki software.

http://www.wikidata.org/

There are several ways to access the data from Wikidata. 
You can access data per item, or the entity of the data as dumps.

aktu

Each item or property has a URI that is obtained by appending its ID (such as Q42 or P12) 
to the Wikidata namespace:

Access data for Item Q42 (Douglas Adams) in JSON format:
https://www.wikidata.org/wiki/Special:EntityData/Q42.json



Couchbase
---------

Couchbase is a open source NoSQL database.

Couchbase - http://www.couchbase.com/

Couchbase Documentation - http://www.couchbase.com/documentation

Start / Stop Couchbase: 
  name@servant:~$ sudo /etc/init.d/couchbase-server start|stop
  
Couchbase in the cloud
  http://www.cloudifysource.org/
  Username: Administrator
  Password: password



Couchbase View example #1: Select all persons
---------------------------------------------

View:

```javascript
function (doc, meta) {
  // Skip documents that aren't JSON
  if (meta.type == "json") {
    if(doc.claims.P31) {
      for( id in doc.claims.P31 ) {
          emit(doc.claims.P31[id].mainsnak.datavalue.value, 
               [doc.id]);
      }
    }
  }
}
```

Key: 
```javascript
{"entity-type":"item","numeric-id":5}
```
 
Url: 
http://localhost:8092/wikidata/_design/cities/_view/cities?key=%7B%22entity-type%22%3A%22item%22%2C%22numeric-id%22%3A5%7D&connection_timeout=60000&limit=10&skip=80



Couchbase View example #2: Select all actors
---------------------------------------------

View:
```javascript
function (doc, meta) {
  // Skip documents that aren't JSON
  if (meta.type == "json") {
    if(doc.claims.P31 && doc.claims.P106) {
      for( p31Id in doc.claims.P31 ) {
        var p31 = doc.claims.P31[p31Id].mainsnak.datavalue.value; 
        for( p106Id in doc.claims.P106 ) {
          var p106 = doc.claims.P106[p106Id].mainsnak.datavalue.value;
          emit([p31,p106], [doc.id,doc.labels.en.value]);
        }
      }     
    }
  }
}
```

Key;
```javascript
[{"entity-type":"item","numeric-id":5},{"entity-type":"item","numeric-id":33999}]
```

Url:
http://localhost:8092/wikidata/_design/actors/_view/actors?key=%5B%7B%22entity-type%22%3A%22item%22%2C%22numeric-id%22%3A5%7D%2C%7B%22entity-type%22%3A%22item%22%2C%22numeric-id%22%3A33999%7D%5D&connection_timeout=60000&limit=1000&skip=0

