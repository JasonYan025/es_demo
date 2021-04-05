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
 * es7.10.2,高级客户端本地测试类
 */
@SpringBootTest
class EsApplicationTests {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    @Autowired
    private ObjectMapper mapper;


    //创建索引
    @Test
    void createIndex() throws IOException {
        //创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("y_index");
        //执行请求,获取响应
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        ObjectMapper objectMapper = new ObjectMapper();
        String res = objectMapper.writeValueAsString(createIndexResponse);
        System.out.println(res);
    }

    //测试获取索引
    @Test
    void indexExist() throws IOException {
        GetIndexRequest request = new GetIndexRequest("y_index");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);

    }

    //删除索引
    @Test
    void deleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("y_index");
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        String s = new ObjectMapper().writeValueAsString(delete);
        System.out.println(s);
    }

    //创建文档
    @Test
    void createDoc() throws IOException {
        User yan = new User("yan", 3);
        //创建请求
        IndexRequest request = new IndexRequest("y_index");
        //request.type("_doc");
        request.id("1");
        request.timeout(TimeValue.timeValueSeconds(1));
        request.source(new ObjectMapper().writeValueAsString(yan), XContentType.JSON);
        IndexResponse index = client.index(request, RequestOptions.DEFAULT);
        System.out.println(new ObjectMapper().writeValueAsString(index));
        System.out.println(index.status());
    }

    //判断文档是否存在
    @Test
    void docExist() throws IOException {
        GetRequest request = new GetRequest("y_index", "1");
        //设置不需要返回_source的上下文
        //request.fetchSourceContext(new FetchSourceContext(false));
        //request.storedFields("_none_");
        boolean exists = client.exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    //获取文档信息
    @Test
    void getDoc() throws IOException {
        GetRequest request = new GetRequest("y_index", "1");
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        System.out.println(new ObjectMapper().writeValueAsString(response));
    }

    //更新文档信息
    @Test
    void updateDoc() throws IOException {
        UpdateRequest request = new UpdateRequest("y_index", "1");
        request.timeout("1s");
        User user = new User("张三", 123);
        request.doc(mapper.writeValueAsString(user), XContentType.JSON);
        UpdateResponse update = client.update(request, RequestOptions.DEFAULT);
        System.out.println(mapper.writeValueAsString(update));
        System.out.println(update.status());

    }

    //删除文档
    @Test
    void deleteDoc() throws IOException {
        DeleteRequest request = new DeleteRequest("y_index", "1");
        DeleteResponse delete = client.delete(request, RequestOptions.DEFAULT);
        System.out.println(mapper.writeValueAsString(delete));
        System.out.println(delete.status());

    }

    //批量插入数据
    @Test
    void batchCreate() throws IOException {
        ArrayList<User> users = new ArrayList<>();
        users.add(new User("张三1", 1));
        users.add(new User("张三2", 2));
        users.add(new User("张三3", 3));
        users.add(new User("张三4", 4));
        users.add(new User("张三5", 5));
        users.add(new User("张三6", 6));

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

    //查询文档
    @Test
    void searchDoc() throws IOException {
        SearchRequest searchRequest = new SearchRequest("y_index");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //查询条件可以使用querybuilders快速匹配
        //termQuery对中文失效
        /*TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "张三");
        sourceBuilder.query(termQueryBuilder);*/

        //termQuery用作中文失效。name.keyword可以精确匹配，不带keyword就是模糊匹配
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("name.keyword", "张三");
        sourceBuilder.query(matchQueryBuilder);

        //sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        //分页
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
