package com.carl.canaltoes;

import java.io.IOException;

import javax.annotation.Resource;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import com.carl.canaltoes.domain.Product;

import cn.hutool.core.lang.UUID;
import cn.hutool.json.JSONUtil;

@SpringBootTest
class CanalToEsApplicationTests {

//    @Autowired
//    private ProductRepository productRepository;

    @Resource
    private RestHighLevelClient elasticsearchClient;

//    @Test
//    public void contextLoads() {
//        List<Product> productList = new ArrayList<>();
//        for (int i = 0; i < 10; i++) {
//            Product product = new Product();
//            product.setId(UUID.fastUUID().toString(true));
//            product.setName("商品" + i);
//            product.setDesc("好商品" + i);
//            productList.add(product);
//        }
//        productRepository.saveAll(productList);
//
//    }

    @Test
    public void select() throws IOException {
        SearchRequest searchRequest = new SearchRequest("product");
        // 设置搜索条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 创建查询构建器
        sourceBuilder.query(QueryBuilders.termQuery("name", "商"));
        // 设置超时时间
        sourceBuilder.timeout(TimeValue.timeValueSeconds(60));
        searchRequest.source(sourceBuilder);
        // 客户端发送请求
        SearchResponse response = elasticsearchClient.search(searchRequest, RequestOptions.DEFAULT);
        for (SearchHit hit : response.getHits().getHits()) {
            System.out.println("----------");
            System.out.println(hit.getSourceAsMap());
        }
    }

    @Test
    public void saveDocumentToIndex() throws IOException {
        Product product = new Product();
        product.setId(UUID.randomUUID().toString(true));
        product.setName("iphone 16 pro max");
        product.setDesc("iphone 16 pro max"+"太贵了");
        IndexRequest indexRequest = new IndexRequest("product");
        indexRequest.source(JSONUtil.toJsonStr(product), XContentType.JSON);
        IndexResponse response = elasticsearchClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(response.getResult());
    }

    @Test
    public void createIndex() throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("user");
        CreateIndexResponse response = elasticsearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
    }

}
