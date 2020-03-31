package cz.zcu.kiv.nlp.ir;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.persistent.StartPersistentTaskAction;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


/**
 * @author tigi, pauli
 */
public class JavaAPIMain {

    private static final Logger log = LoggerFactory.getLogger(JavaAPIMain.class);

    /**
     * Size of the batch of documents to be uploaded.
     */
    public static final int BULK_SIZE = 2000;

    public static final String DATA_FOLDER = "data";

    //name of index
    static final String indexName = "rpol-comments";

    //name of type
    static final String typeName = "comment";

    static final String id1 = "1";
    static final String id2 = "2";


    public static void main(String args[]) throws IOException {

        AbstractClient client = createClient("valesz-cluster", "localhost", 9300);
        printConnectionInfo(client);

        setIndexMapping(client, indexName);

        List<String> jsonDocuments = loadJsonData("rpol-comments.json");
        if (jsonDocuments.isEmpty()) {
            log.warn("No documents to index.");
            return;
        }

        indexDocuments(jsonDocuments, client, indexName);


        //2) XContentBuilder - ES helper - same as 1) but with different approach
//        XContentBuilder xContent = createJsonDocument("ElasticSearch: Java",
//                "ElasticSeach provides Java API, thus it executes all operations asynchronously by using client object..",
//                        new Date(),
//                        new String[]{"elasticsearch","Apache","Lucene"},
//                "Hüseyin Akdoğan");
//        docsXContent.add(xContent);
//
//        requestBuilder = client.prepareIndex(indexName, typeName, id1);
//        response = requestBuilder.setSource(xContent).get();
//        printResponse(response);
//
//
//        xContent = createJsonDocument("Java Web Application and ElasticSearch (Video)",
//                "Today, here I am for exemplifying the usage of ElasticSearch which is an open source, distributed " +
//                        "and scalable full text search engine and a data analysis tool in a Java web application.",
//                        new Date(),
//                        new String[]{"elasticsearch"},
//                "Hüseyin Akdoğan");
//        docsXContent.add(xContent);
//
//
//        requestBuilder = client.prepareIndex(indexName, typeName, id2);
//        response = requestBuilder.setSource(xContent).get();
//        printResponse(response);
//
//
//
//        //get document by id
//        getDocument(client, indexName, typeName, id1);
//        getDocument(client, indexName, typeName, "3");
//
//
//        //update documents
//        try {
//            updateDocument(client, indexName, typeName, id1, "title", "ElasticSearch: Java API");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        getDocument(client, indexName, typeName, id1);
//
//
//
//        try {
//            updateDocument(client, indexName, typeName, id1, "tags", new String[]{"bigdata", "really"});
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        getDocument(client, indexName, typeName, id1);
//
//
//
//        //searching documents
//        System.out.println("");
//        searchDocument(client, indexName, typeName, "title", "ElasticSearch");
//        searchDocument(client, indexName, typeName, "content", "provides Java API");
//        searchPhrase(client, indexName, typeName, "content", "provides Java API");
//
//
//        //delete documents
//        deleteDocument(client, indexName, typeName, id1);
//        getDocument(client, indexName, typeName, id1);

        //bulk example
        //bulkIndex(docsXContent, client);

        //close connection
        client.close();
    }

    private static void setIndexMapping(AbstractClient client, String indexName) throws IOException {
        log.debug("Setting mapping in index '{}'.", indexName);

        PutMappingRequest request = new PutMappingRequest(indexName);
        String mappingJson = loadMapping("rpol-comment-mapping.json");
        request.source(mappingJson, XContentType.JSON);
        client.admin().indices().putMapping(request);

        log.debug("Done.");
    }

    private static String loadMapping(String mappingFileName) throws IOException {
        return String.join("",Files.readAllLines(new File(DATA_FOLDER+"/"+mappingFileName).toPath()));
    }

    private static void indexDocuments(List<String> jsonDocuments, AbstractClient client, String indexName) {
        BulkRequest request = new BulkRequest();
        int currentBulkSize = 0;
        int bulkCntr = 0;
        int cntr = 0;
        final int docCount = jsonDocuments.size();

        log.debug("Indexing {} documents.", jsonDocuments.size());

        for (String jsonDocument : jsonDocuments) {
            request.add(new IndexRequest(indexName).source(XContentType.JSON, jsonDocument));
            currentBulkSize++;
            cntr++;

            // if the bulk size was reached of last document was processed
            // execute the request
            if (currentBulkSize == BULK_SIZE || cntr == (docCount-1)) {
                bulkCntr++;
                log.debug("Executing bulk {} with {} documents.", bulkCntr, currentBulkSize);

                client.bulk(request);
                request = new BulkRequest();
                currentBulkSize = 0;
            }
        }
    }

    private static AbstractClient createClient(String clusterName, String host, int port) throws UnknownHostException {
        Settings settings = Settings.builder()
                .put("cluster.name", clusterName)
                .build();
        TransportClient client = new PreBuiltTransportClient(settings);

//        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName(host), port));
        return client;
    }

    private static List<String> loadJsonData(String fileName) throws IOException {
        return Files.readAllLines(new File(DATA_FOLDER+"/"+fileName).toPath());
    }

    public static void searchDocument(Client client, String index, String type,
                                      String field, String value) {

        //SearchType.DFS_QUERY_THEN_FETCH - more
        //https://www.elastic.co/blog/understanding-query-then-fetch-vs-dfs-query-then-fetch
        log.info("Searching \"" + value + "\" in field:" + "\"" + field + "\"");
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)     //try change to SearchType.QUERY_THEN_FETCH - see the change in score
                .setQuery(QueryBuilders.matchQuery(field, value)) //Query match - simplest query
                .setFrom(0).setSize(30)                         //can be used for pagination
                .setExplain(true)
                .get();

        //print response
        printSearchResponse(response);

    }

    public static void searchPhrase(Client client, String index, String type,
                                    String field, String value) {
        log.info("Searching phrase \"" + value + "\" in field:" + "\"" + field + "\"");
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchPhraseQuery(field, value))
                .setFrom(0).setSize(30)
                .setExplain(true)
                .get();

        printSearchResponse(response);
    }

    private static void printSearchResponse(SearchResponse response) {
        SearchHit[] results = response.getHits().getHits();
        log.info("Search complete");
        log.info("Search took: " + response.getTook().getMillis() + " ms");
        log.info("Found documents: " + response.getHits().getTotalHits());

        for (SearchHit hit : results) {
            log.info("--------");
            log.info("Doc id: " + hit.getId());
            log.info("Score: " + hit.getScore());
            String result = hit.getSourceAsString();
            log.info(result);
            //hit.getSourceAsMap();
        }
        log.info("------------------------------");
        System.out.println("");
    }

    private static void bulkIndex(List<XContentBuilder> docsXcontent, Client client) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        bulkRequest.add(client.prepareIndex(indexName, typeName, id1).setSource(docsXcontent.get(0)));
        bulkRequest.add(client.prepareIndex(indexName, typeName, id2).setSource(docsXcontent.get(1)));

        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            log.info("Error - Bulk Indexing");
        }else {
            log.info("Bulk index request complete");
        }

        //update bulk etc..
        //bulkRequest.add(client.prepareUpdate(indexName,typeName,id))
    }

    private static void printResponse(IndexResponse response) {
        // Index name
        String _index = response.getIndex();
        // Type name
        String _type = response.getType();
        // Document ID (generated or not)
        String _id = response.getId();
        // Version (if it's the first time you index this document, you will get: 1)
        long _version = response.getVersion();
        // isCreated() is true if the document is a new one, false if it has been updated
        boolean created = response.status() == RestStatus.CREATED;
        log.info("Doc indexed to index: " + _index + " type: " + _type + " id: " + _id + " version: " + _version + " created: " + created);
    }

    public static Map<String, Object> putJsonDocument(String title, String content, Date postDate,
                                                      String[] tags, String author) {

        Map<String, Object> jsonDocument = new HashMap<String, Object>();

        jsonDocument.put("title", title);
        jsonDocument.put("content", content);
        jsonDocument.put("postDate", postDate);
        jsonDocument.put("tags", tags);
        jsonDocument.put("author", author);

        return jsonDocument;
    }

    public static XContentBuilder createJsonDocument(String title, String content, Date postDate,
                                            String[] tags, String author) throws IOException {
        XContentBuilder builder = jsonBuilder();
        builder.startObject()
                .field("title", title)
                .field("content", content)
                .field("postDate", postDate)
                .field("tags", tags)
                .field("author", author)
                .field("user", "kimchy")
                .field("message", "trying out Elasticsearch")
                .endObject();

        //        log.info("Generated JSON to index:" + builder.prettyPrint().string());
        return builder;
    }

    public static void getDocument(Client client, String index, String type, String id) {

        GetResponse getResponse = client.prepareGet(index, type, id).get();

        if (!getResponse.isExists()) {
            log.info("Document with id:" + id + " not found");
            return;
        }

        Map<String, Object> source = getResponse.getSource();

        log.info("------------------------------");
        log.info("Retrieved document");
        log.info("Index: " + getResponse.getIndex());
        log.info("Type: " + getResponse.getType());
        log.info("Id: " + getResponse.getId());
        log.info("Version: " + getResponse.getVersion());
        log.info("Document title: " + source.get("title"));
        log.info(source.toString());
        log.info("------------------------------");

        //parsing - mannualy, deserialize JSON to object...
        String title = (String)source.get("title");
        String content = (String)source.get("content");


    }

    //allows partial updates  - not whole doc
    public static void updateDocument(Client client, String index, String type,
                                      String id, String field, Object newValue) throws IOException, ExecutionException, InterruptedException {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(index);
        updateRequest.type(type);
        updateRequest.id(id);
        updateRequest.doc(jsonBuilder()
                .startObject()
                .field(field, newValue)
                .endObject());

        client.update(updateRequest).get();
    }

    //alternative update
    public static void prepareUpdateDocument(Client client, String index, String type,
                                             String id, String field, Object newValue) throws IOException {
        client.prepareUpdate(index, type, id)
                .setDoc(jsonBuilder()
                        .startObject()
                        .field(field, newValue)
                        .endObject())
                .get();
    }


    public static void deleteDocument(Client client, String index, String type, String id) {

        DeleteResponse response = client.prepareDelete(index, type, id).get();
        log.info("Information on the deleted document:");
        log.info("Index: " + response.getIndex());
        log.info("Type: " + response.getType());
        log.info("Id: " + response.getId());
        log.info("Version: " + response.getVersion());
    }

    private static final void printConnectionInfo(Client client) {
        ClusterHealthResponse health = client.admin().cluster().prepareHealth().get();
        String clusterName = health.getClusterName();

        log.info("Connected to Cluster: " + clusterName);
        log.info("Indices in cluster: ");
        for (ClusterIndexHealth heal : health.getIndices().values()) {
            String index = heal.getIndex();
            int numberOfShards = heal.getNumberOfShards();
            int numberOfReplicas = heal.getNumberOfReplicas();
            ClusterHealthStatus status = heal.getStatus();

            log.info("Index: " + index);
            log.info("Status: " + status.toString());
            log.info("Number of Shards: " + numberOfShards);
            log.info("Number of Replicas: " + numberOfReplicas);
            log.info("---------");

        }

    }
}
