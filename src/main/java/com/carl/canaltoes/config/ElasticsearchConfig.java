package com.carl.canaltoes.config;


import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;

/**
 * @author: carl
 * @date: 2025/1/15
 */
@Component
@Configuration
public class ElasticsearchConfig{


    @Bean("myElasticsearchClient")
    public ElasticsearchClient myElasticsearchClient(){
        RestClient httpClient = RestClient.builder(
                new HttpHost("localhost", 9200)
        ).build();
        ElasticsearchTransport transport = new RestClientTransport(
                httpClient,
                new JacksonJsonpMapper()
        );

        return new ElasticsearchClient(transport);
    }
}
