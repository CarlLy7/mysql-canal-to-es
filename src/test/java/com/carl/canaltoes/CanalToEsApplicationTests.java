package com.carl.canaltoes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import com.carl.canaltoes.domain.Product;
import com.carl.canaltoes.domain.User;

import cn.hutool.core.lang.UUID;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOptionsBuilders;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
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

    @Test
    public void fuzzyQueryTest() throws IOException {
        SearchRequest.Builder builder = new SearchRequest.Builder();
        builder.index("user");
        builder.query(QueryBuilders.fuzzy(f -> f.field("like").value("中文国际").fuzziness("2")));
        SearchResponse<User> response = myElasticsearchClient.search(builder.build(), User.class);
        System.out.println(response.toString());
    }

    @Test
    public void matchPhraseTest() throws IOException {
        SearchRequest.Builder builder = new SearchRequest.Builder();
        builder.index("user");
        builder.query(QueryBuilders.matchPhrase(m -> m.field("like").query("广州白云").slop(2)));
        SearchResponse<User> response = myElasticsearchClient.search(builder.build(), User.class);
        System.out.println(response.toString());
    }

    @Test
    public void mustQueryTest() throws IOException {
        SearchRequest.Builder builder = new SearchRequest.Builder();
        Query queryMatch1 = QueryBuilders.match(m -> m.field("like").query("广州白云山"));
        Query queryMatch2 = QueryBuilders.match(m -> m.field("username").query("carl2"));
        List<Query> matchQuerys=new ArrayList<>();
        matchQuerys.add(queryMatch1);
        matchQuerys.add(queryMatch2);
        builder.index("user");
        builder.query(QueryBuilders.bool(b->b.must(matchQuerys)));
        SearchResponse<User> response = myElasticsearchClient.search(builder.build(), User.class);
        System.out.println(response.toString());
    }

    @Test
    public void shouldQueryTest() throws IOException {
        SearchRequest.Builder builder = new SearchRequest.Builder();
        builder.index("user");
        List<Query> shouldQuerys=new ArrayList<>();
        Query match1 = QueryBuilders.match(m -> m.field("like").query("广州白云山"));
        Query match2 = QueryBuilders.match(m -> m.field("like").query("中威国际"));
        shouldQuerys.add(match1);
        shouldQuerys.add(match2);
        // 根据查询结果中的得分进行倒叙排列
        builder.query(QueryBuilders.bool(b->b.should(shouldQuerys))).sort(SortOptionsBuilders.score(s->s.order(SortOrder.Desc)));
        SearchResponse<User> response = myElasticsearchClient.search(builder.build(), User.class);
        for (Hit<User> hit : response.hits().hits()) {
            System.out.println("结果："+hit.toString());
        }
    }

    @Test
    public void highSearchTest() throws IOException {
        SearchRequest.Builder builder = new SearchRequest.Builder();
        builder.index("user");
        builder.query(QueryBuilders.fuzzy(f->f.field("like").value("中文国际").fuzziness("2")));
        builder.highlight(h->h.preTags("<span style='color:red'>").postTags("</span>").fields("like",i->i.numberOfFragments(0)));
        SearchResponse<User> response = myElasticsearchClient.search(builder.build(), User.class);
        for (Hit<User> hit : response.hits().hits()) {
            System.out.println("内容为: "+hit.toString());
        }
    }


}
