package com.carl.canaltoes.service;

import java.io.IOException;
import java.util.List;

import javax.annotation.Resource;

import org.springframework.stereotype.Service;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.carl.canaltoes.domain.User;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.*;

/**
 * @author: carl
 * @date: 2025/1/15
 */
@Service
public class EsService {
    @Resource
    private ElasticsearchClient myElasticsearchClient;

    public void saveToIndex(List<CanalEntry.Column> columns) {

        JSONObject jsonObject = new JSONObject();
        for (CanalEntry.Column column : columns) {
            jsonObject.put(column.getName(), column.getValue());
        }
        try {
            User user = JSONUtil.toBean(jsonObject, User.class);
            IndexRequest.Builder<Object> builder = new IndexRequest.Builder<>();
            builder.index("user").id(user.getId()).document(user);
            IndexResponse response = myElasticsearchClient.index(builder.build());
            System.out.println(response.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void updateToIndex(List<CanalEntry.Column> columns) {
        JSONObject jsonObject = new JSONObject();
        for (CanalEntry.Column column : columns) {
            jsonObject.put(column.getName(), column.getValue());
        }
        try {
            User user = JSONUtil.toBean(jsonObject, User.class);
            UpdateRequest.Builder builder = new UpdateRequest.Builder<>();
            builder.index("user").id(user.getId()).doc(user);
            UpdateResponse response = myElasticsearchClient.update(builder.build(), User.class);
            System.out.println(response.toString());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deleteToIndex(List<CanalEntry.Column> columns) {
        JSONObject jsonObject = new JSONObject();
        for (CanalEntry.Column column : columns) {
            jsonObject.put(column.getName(), column.getValue());
        }
        try {
            User user = JSONUtil.toBean(jsonObject, User.class);
            DeleteRequest.Builder builder = new DeleteRequest.Builder();
            builder.index("user").id(user.getId());
            DeleteResponse response = myElasticsearchClient.delete(builder.build());
            System.out.println(response.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
