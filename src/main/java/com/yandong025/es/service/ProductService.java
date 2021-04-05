package com.yandong025.es.service;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.impl.BeanPropertyMap;
import com.yandong025.es.pojo.Product;
import com.yandong025.es.util.HtmlParseUtil;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.jsoup.internal.StringUtil;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cglib.beans.BeanMap;
import org.springframework.stereotype.Service;
import org.thymeleaf.util.StringUtils;

import javax.swing.text.Highlighter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class ProductService {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Autowired
    private ObjectMapper mapper;

    public boolean parseContent(String keyword) throws IOException {
        List<Product> products = HtmlParseUtil.getProductByKeyWord(keyword);
        BulkRequest bulkRequest = new BulkRequest();
        for (Product product : products) {
            bulkRequest.add(
                    new IndexRequest("jd_product")
                            .id(product.getId())
                            .source(mapper.writeValueAsString(product), XContentType.JSON));
        }
        BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        return !bulk.hasFailures();
    }


    public List<Product> getProduct(String keyword, int pageNo, int size) throws IOException {
        SearchRequest searchRequest = new SearchRequest("jd_product");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        MatchQueryBuilder query = QueryBuilders.matchQuery("title", keyword).minimumShouldMatch("100%");
        //设置分页
        sourceBuilder.query(query);
        sourceBuilder.from(pageNo);
        sourceBuilder.size(size);

        //设置高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("title");
        highlightBuilder.preTags("<span style='color:red'>");
        highlightBuilder.postTags("</span>");
        sourceBuilder.highlighter(highlightBuilder);

        sourceBuilder.size(size);
        searchRequest.source(sourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = search.getHits().getHits();
        List<Product> products = new ArrayList();
        for (SearchHit hit : hits) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            HighlightField title = highlightFields.get("title");
            Text[] fragments = title.fragments();
            String new_title = "";
            for (Text fragment : fragments) {
                new_title += fragment;
            }
            Product product = mapper.readValue(mapper.writeValueAsString(sourceAsMap), Product.class);
            if (!StringUtils.isEmpty(new_title)) {
                product.setTitle(new_title);
            }
            products.add(product);
        }

        if (products.size() == 0) {
            parseContent(keyword);
        }
        return products;
    }

}
