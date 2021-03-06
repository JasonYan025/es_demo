package com.yandong025.es;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yandong025.es.pojo.User;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * es7.10.2,??????????????????????????????
 */
@SpringBootTest
class EsApplicationTests {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    @Autowired
    private ObjectMapper mapper;


    //????????????
    @Test
    void createIndex() throws IOException {
        //??????????????????
        CreateIndexRequest request = new CreateIndexRequest("y_index");
        //????????????,????????????
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        ObjectMapper objectMapper = new ObjectMapper();
        String res = objectMapper.writeValueAsString(createIndexResponse);
        System.out.println(res);
    }

    //??????????????????
    @Test
    void indexExist() throws IOException {
        GetIndexRequest request = new GetIndexRequest("y_index");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);

    }

    //????????????
    @Test
    void deleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("y_index");
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        String s = new ObjectMapper().writeValueAsString(delete);
        System.out.println(s);
    }

    //????????????
    @Test
    void createDoc() throws IOException {
        User yan = new User("yan", 3);
        //????????????
        IndexRequest request = new IndexRequest("y_index");
        //request.type("_doc");
        request.id("1");
        request.timeout(TimeValue.timeValueSeconds(1));
        request.source(new ObjectMapper().writeValueAsString(yan), XContentType.JSON);
        IndexResponse index = client.index(request, RequestOptions.DEFAULT);
        System.out.println(new ObjectMapper().writeValueAsString(index));
        System.out.println(index.status());
    }

    //????????????????????????
    @Test
    void docExist() throws IOException {
        GetRequest request = new GetRequest("y_index", "1");
        //?????????????????????_source????????????
        //request.fetchSourceContext(new FetchSourceContext(false));
        //request.storedFields("_none_");
        boolean exists = client.exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    //??????????????????
    @Test
    void getDoc() throws IOException {
        GetRequest request = new GetRequest("y_index", "1");
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        System.out.println(new ObjectMapper().writeValueAsString(response));
    }

    //??????????????????
    @Test
    void updateDoc() throws IOException {
        UpdateRequest request = new UpdateRequest("y_index", "1");
        request.timeout("1s");
        User user = new User("??????", 123);
        request.doc(mapper.writeValueAsString(user), XContentType.JSON);
        UpdateResponse update = client.update(request, RequestOptions.DEFAULT);
        System.out.println(mapper.writeValueAsString(update));
        System.out.println(update.status());

    }

    //????????????
    @Test
    void deleteDoc() throws IOException {
        DeleteRequest request = new DeleteRequest("y_index", "1");
        DeleteResponse delete = client.delete(request, RequestOptions.DEFAULT);
        System.out.println(mapper.writeValueAsString(delete));
        System.out.println(delete.status());

    }

    //??????????????????
    @Test
    void batchCreate() throws IOException {
        ArrayList<User> users = new ArrayList<>();
        users.add(new User("??????1", 1));
        users.add(new User("??????2", 2));
        users.add(new User("??????3", 3));
        users.add(new User("??????4", 4));
        users.add(new User("??????5", 5));
        users.add(new User("??????6", 6));

        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < users.size(); i++) {
            bulkRequest.add(
                    new IndexRequest("y_index")
                            .source(mapper.writeValueAsString(users.get(i)), XContentType.JSON)
            );
        }
        BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(mapper.writeValueAsString(bulk));
        System.out.println(bulk.status());
        System.out.println(bulk.hasFailures());

    }

    //????????????
    @Test
    void searchDoc() throws IOException {
        SearchRequest searchRequest = new SearchRequest("y_index");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //????????????????????????querybuilders????????????
        //termQuery???????????????
        /*TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "??????");
        sourceBuilder.query(termQueryBuilder);*/

        //termQuery?????????????????????name.keyword???????????????????????????keyword??????????????????
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("name.keyword", "??????");
        sourceBuilder.query(matchQueryBuilder);

        //sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        //??????
        //searchSourceBuilder.from(1);
        //searchSourceBuilder.size(1);
        searchRequest.source(sourceBuilder);
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("res:" + mapper.writeValueAsString(search));
        for (SearchHit hit : search.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
        client.close();
    }

}
