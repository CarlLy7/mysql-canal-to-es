package com.carl.canaltoes.config;


import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.RestClients;
import org.springframework.data.elasticsearch.config.AbstractElasticsearchConfiguration;
import org.springframework.stereotype.Component;

/**
 * @author: carl
 * @date: 2025/1/15
 */
@Component
@Configuration
public class ElasticsearchConfig extends AbstractElasticsearchConfiguration {

    @Override
    @Bean
    public RestHighLevelClient elasticsearchClient() {
        ClientConfiguration clientConfiguration = ClientConfiguration.builder().connectedTo("localhost:9200").build();
        return RestClients.create(clientConfiguration).rest();
    }
}
