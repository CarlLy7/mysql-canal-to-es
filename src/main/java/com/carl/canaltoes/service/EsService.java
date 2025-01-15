package com.carl.canaltoes.service;

import java.util.List;

import javax.annotation.Resource;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.xcontent.XContentType;
import org.springframework.stereotype.Service;

import com.alibaba.otter.canal.protocol.CanalEntry;

/**
 * @author: carl
 * @date: 2025/1/15
 */
@Service
public class EsService {
    @Resource
    private RestHighLevelClient elasticsearchClient;

    public void saveToIndex(List<CanalEntry.Column> columns){
        StringBuilder sb=new StringBuilder();
        sb.append("{");
        for (CanalEntry.Column column : columns) {
            sb.append("\"").append(column.getName()).append("\":\"").append(column.getValue()).append("\",");
        }
        sb.deleteCharAt(sb.length()-1);
        sb.append("}");
        IndexRequest indexRequest = new IndexRequest("user")
                .source(sb.toString(),  XContentType.JSON);

        try {
            elasticsearchClient.index(indexRequest,  RequestOptions.DEFAULT);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void updateToIndex(List<CanalEntry.Column> columns){
        StringBuilder sb=new StringBuilder();
        sb.append("{");
        for (CanalEntry.Column column : columns) {
            sb.append("\"").append(column.getName()).append("\":\"").append(column.getValue()).append("\",");
        }
        sb.deleteCharAt(sb.length()-1);
        sb.append("}");

        try {
            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.index("user");
            updateRequest.doc(sb.toString(),XContentType.JSON);
            elasticsearchClient.update(updateRequest,RequestOptions.DEFAULT);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void deleteToIndex(List<CanalEntry.Column> columns){
        for (CanalEntry.Column column : columns) {
            try{
                DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest("user");
                deleteByQueryRequest.setQuery(QueryBuilders.matchQuery(column.getName(),column.getValue()));
                BulkByScrollResponse deleteResult = elasticsearchClient.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);
                long deletedDocs = deleteResult.getDeleted();
                System.out.println("Deleted  " + deletedDocs + " documents.");
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
