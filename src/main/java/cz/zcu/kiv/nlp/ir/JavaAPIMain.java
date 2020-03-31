package cz.zcu.kiv.nlp.ir;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


/**
 * @author tigi, pauli
 */
public class JavaAPIMain {

    private static final Logger log = LoggerFactory.getLogger(JavaAPIMain.class);

    /**
     * Size of the batch of documents to be uploaded.
     */
    private static final int BULK_SIZE = 2000;

    private static final String DATA_FOLDER = "data";

    public static final String DATA_ORIGINAL_DATE_FORMAT = "EEE MMM dd HH:mm:ss yyyy z";
    public static final String DATA_ELASTIC_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZZ";

    private static final String SEARCH_MODE = "search";
    private static final String UPDATE_MODE = "update";

    //name of indexes
    private static final String INDEX_NAME = "rpol-comments";
    private static final String TEST_INDEX_NAME = "test-index";

    private static final String TEST_DOCUMENT = "{\"username\":\"TwilitSky\", \"text\":\"They should be able to get it. They did last time except for Rand \\\"Baby Cracked Ribs\\\" Paul and Mike Lee of Utah.\", \"score\":25, \"timestamp\":\"Tue Feb 25 00:49:25 2020 UTC\"}";
    private static final String TEST_DOCUMENT_ID = "td-1";

    public static void main(String args[]) throws IOException, ExecutionException, InterruptedException {

        RestHighLevelClient client = createClient("valesz-cluster", "localhost", 9200);
        printConnectionInfo(client);

        try {
            createIndexIfNotExist(client, INDEX_NAME);

            if (isSearchMode(args)) {
                log.debug("Search mode.");
                searchTrumpBad(client, INDEX_NAME);
                searchTrumpGood(client, INDEX_NAME);
                searchTrumpGoodFilterByScore(client, INDEX_NAME, 50);
            } else if (isUpdateMode(args)) {
                log.debug("Update mode.");
                indexTestDocument(client, TEST_INDEX_NAME, TEST_DOCUMENT, TEST_DOCUMENT_ID);
                getDocument(client, TEST_INDEX_NAME, TEST_DOCUMENT_ID);
                updateDocument(client, TEST_INDEX_NAME, TEST_DOCUMENT_ID, "text", "This is updated text.");
                getDocument(client, TEST_INDEX_NAME, TEST_DOCUMENT_ID);
                deleteDocument(client, TEST_INDEX_NAME, TEST_DOCUMENT_ID);
            } else {
                log.debug("Standard mode. Uploading data.");
                setIndexMapping(client, INDEX_NAME);

                List<String> jsonDocuments = loadJsonData("rpol-comments.json");
                if (jsonDocuments.isEmpty()) {
                    log.warn("No documents to index.");
                } else {
                    indexDocuments(jsonDocuments, client, INDEX_NAME);
                }

            }
        } catch (Exception ex) {
            log.error("Exception: ", ex);
        }

        client.close();
    }

    private static void createIndexIfNotExist(RestHighLevelClient client, String indexName) throws IOException {
        log.debug("Checking if index '{}' exists.", indexName);

        GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
        boolean indexExists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);

        if (!indexExists) {
            log.debug("Index does not exist, creating new one.");
            client.indices().create(new CreateIndexRequest(indexName), RequestOptions.DEFAULT);
        }
    }

    private static void indexTestDocument(RestHighLevelClient client, String indexName, String jsonDocument, String documentId) throws IOException {
        log.debug("Indexing test document to index '{}'", indexName);
        client.index(new IndexRequest(indexName).id(documentId).source(jsonDocument, XContentType.JSON),
                RequestOptions.DEFAULT);
        log.debug("Done.");
    }

    private static boolean isUpdateMode(String[] args) {
        return args.length > 0 && UPDATE_MODE.equals(args[0]);
    }

    /**
     * Searches for documents containing 'good' mentions of Donald Trump whose score
     * is at least minScoreCount.
     * @param client
     * @param indexName
     * @param minCommentScore
     */
    private static void searchTrumpGoodFilterByScore(RestHighLevelClient client, String indexName, int minCommentScore) throws IOException {
        log.debug("Performing 'trump good' query with reddit score at least '{}'.", minCommentScore);
        org.elasticsearch.index.query.BoolQueryBuilder qb = QueryBuilders.boolQuery();
        qb.must().add(QueryBuilders.matchQuery("text", "trump"));
        qb.should(QueryBuilders.matchQuery("text", "good blessed best capable leader savior greatest clever smartest hero"));
        qb.minimumShouldMatch(1);
        qb.filter(QueryBuilders.rangeQuery("score").gte(minCommentScore));

        SearchResponse response = executeSearchRequest(client, indexName, qb);

        //print response
        printSearchResponse(response);
    }

    /**
     * Searches for documents containing 'good' mentions of Donald Trump.
     * @param client
     * @param indexName
     */
    private static void searchTrumpGood(RestHighLevelClient client, String indexName) throws IOException {
        log.debug("Performing 'trump good' query.");
        org.elasticsearch.index.query.BoolQueryBuilder qb = QueryBuilders.boolQuery();
        qb.must().add(QueryBuilders.matchQuery("text", "trump"));
        qb.should(QueryBuilders.matchQuery("text", "good blessed best capable leader savior greatest clever smartest hero"));
        qb.minimumShouldMatch(1);

        SearchResponse response = executeSearchRequest(client, indexName, qb);
        //print response
        printSearchResponse(response);
    }

    /**
     * Searches for documents containing 'bad' mentions of Donald Trump.
     * @param client
     * @param indexName
     */
    private static void searchTrumpBad(RestHighLevelClient client, String indexName) throws IOException {
        log.debug("Performing 'trump bad' query.");
        org.elasticsearch.index.query.BoolQueryBuilder qb = QueryBuilders.boolQuery();
        qb.must().add(QueryBuilders.matchQuery("text", "trump"));
        qb.should(QueryBuilders.matchQuery("text", "bad traitor horrible worst conman incapable incompetent evil"));
        qb.minimumShouldMatch(1);

        SearchResponse response = executeSearchRequest(client, indexName, qb);
        //print response
        printSearchResponse(response);
    }

    private static SearchResponse executeSearchRequest(RestHighLevelClient client, String indexName, QueryBuilder queryBuilder) throws IOException {
        SearchRequest sr = new SearchRequest(indexName);
        sr.source(new SearchSourceBuilder()
                .query(queryBuilder));

        return client.search(sr, RequestOptions.DEFAULT);
    }

    private static boolean isSearchMode(String[] args) {
        return args.length > 0 && SEARCH_MODE.equals(args[0]);
    }

    private static void setIndexMapping(RestHighLevelClient client, String indexName) throws IOException {
        log.debug("Setting mapping in index '{}'.", indexName);

        PutMappingRequest request = new PutMappingRequest(indexName);
        String mappingJson = loadMapping("rpol-comment-mapping.json");
        request.source(mappingJson, XContentType.JSON);
        client.indices().putMapping(request, RequestOptions.DEFAULT);

        log.debug("Done.");
    }

    private static String loadMapping(String mappingFileName) throws IOException {
        return String.join("",Files.readAllLines(new File(DATA_FOLDER+"/"+mappingFileName).toPath()));
    }

    private static void indexDocuments(List<String> jsonDocuments, RestHighLevelClient client, String indexName) throws IOException {
        BulkRequest request = new BulkRequest();
        int currentBulkSize = 0;
        int bulkCntr = 0;
        int cntr = 0;
        final int docCount = jsonDocuments.size();

        log.debug("Indexing {} documents.", jsonDocuments.size());

        for (String jsonDocument : jsonDocuments) {
            request.add(new IndexRequest(indexName).source(jsonDocument, XContentType.JSON));
            currentBulkSize++;
            cntr++;

            // if the bulk size was reached of last document was processed
            // execute the request
            if (currentBulkSize == BULK_SIZE || cntr == docCount) {
                bulkCntr++;
                log.debug("Executing bulk {} with {} documents.", bulkCntr, currentBulkSize);

                client.bulk(request, RequestOptions.DEFAULT);
                request = new BulkRequest();
                currentBulkSize = 0;
            }
        }
    }

    private static RestHighLevelClient createClient(String clusterName, String host, int port) throws UnknownHostException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost(host, port, "http")
        ));
        return client;
    }

    /**
     * Loads data from json file and transforms the timestamps to correct date format.
     * @param fileName
     * @return
     * @throws IOException
     */
    private static List<String> loadJsonData(String fileName) throws IOException {
        log.debug("Loading documents from '{}'.", fileName);
        List<String> jsonDocumentsPreTransform = Files.readAllLines(new File(DATA_FOLDER+"/"+fileName).toPath());
        List<String> jsonDocumentsTransformed = new ArrayList<>();
        ObjectMapper mapperFrom = new ObjectMapper();
        mapperFrom.setDateFormat(new SimpleDateFormat(DATA_ORIGINAL_DATE_FORMAT, Locale.ENGLISH));

        ObjectMapper mapperTo = new ObjectMapper();
        mapperTo.setDateFormat(new SimpleDateFormat(DATA_ELASTIC_DATE_FORMAT, Locale.ENGLISH));

        Iterator<String> documentsToTransformIt = jsonDocumentsPreTransform.iterator();
        while (documentsToTransformIt.hasNext()) {
            String jsonToTransform = documentsToTransformIt.next();
            documentsToTransformIt.remove();

            Comment c = mapperFrom.readValue(jsonToTransform, Comment.class);
            jsonDocumentsTransformed.add(mapperTo.writeValueAsString(c));
        }

        return jsonDocumentsTransformed;
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

    private static void getDocument(RestHighLevelClient client, String index, String id) throws IOException {

        GetRequest getRequest = new GetRequest(index).id(id);
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);

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
        log.info(source.toString());
        log.info("------------------------------");
    }

    //allows partial updates  - not whole doc
    private static void updateDocument(RestHighLevelClient client, String index,
                                       String id, String field, Object newValue) throws IOException, ExecutionException, InterruptedException {
        log.debug("Updating document '{}'.", id);

        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(index);
        updateRequest.id(id);
        updateRequest.doc(jsonBuilder()
                .startObject()
                .field(field, newValue)
                .endObject());

        client.update(updateRequest, RequestOptions.DEFAULT);

        log.debug("Done.");
    }

    private static void deleteDocument(RestHighLevelClient client, String index, String id) throws IOException {

        log.debug("Deleting document with id '{}'.", id);

        DeleteRequest deleteRequest = new DeleteRequest();
        deleteRequest.index(index);
        deleteRequest.id(id);

        DeleteResponse response = client.delete(deleteRequest, RequestOptions.DEFAULT);
        log.info("Information on the deleted document:");
        log.info("Index: " + response.getIndex());
        log.info("Id: " + response.getId());
        log.info("Version: " + response.getVersion());
    }

    private static void printConnectionInfo(RestHighLevelClient client) throws IOException {
        ClusterHealthResponse health = client.cluster().health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
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
