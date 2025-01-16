package com.carl.canaltoes;

import java.io.IOException;

import javax.annotation.Resource;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import com.carl.canaltoes.domain.Product;

import cn.hutool.core.lang.UUID;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;

@SpringBootTest
class CanalToEsApplicationTests {

    @Resource
    private ElasticsearchClient myElasticsearchClient;

    @Test
    public void createIndex() throws IOException {
        co.elastic.clients.elasticsearch.core.IndexRequest.Builder<Object> builder =
            new co.elastic.clients.elasticsearch.core.IndexRequest.Builder<>();
        builder.index("product");
        Product product = new Product(UUID.fastUUID().toString(true), "商品之城最好的商品", "good good");
        builder.document(product);
        builder.id(UUID.fastUUID().toString(true));
        myElasticsearchClient.index(builder.build());
    }

    @Test
    public void searchTest() throws IOException {
        SearchRequest.Builder builder = new SearchRequest.Builder();
        builder.index("product");
        // wildCard：模糊查询
        builder.query(QueryBuilders.wildcard(w -> w.field("name").value("最好")));
        builder.highlight(h -> h.fields("name", hf -> hf.numberOfFragments(0)));
        SearchResponse<Product> searchResponse = myElasticsearchClient.search(builder.build(), Product.class);
        for (Hit<Product> hit : searchResponse.hits().hits()) {
            System.out.println("结果为: " + hit);
        }
    }

}
