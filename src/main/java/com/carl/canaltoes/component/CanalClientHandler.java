package com.carl.canaltoes.component;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.carl.canaltoes.service.EsService;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author: carl
 * @date: 2025/1/14
 */
@Component
public class CanalClientHandler implements InitializingBean {

    @Resource
    private EsService esService;

    @Override
    @Bean
    public void afterPropertiesSet() throws Exception {
        // 创建链接
        CanalConnector connector =
            CanalConnectors.newSingleConnector(new InetSocketAddress("127.0.0.1", 11111), "example", "", "");
        int batchSize = 1000;
        try {
            connector.connect();
            connector.subscribe(".*\\..*");
            connector.rollback();
            while (true) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                this.toES(message.getEntries());
                long batchId = message.getId();
                int size = message.getEntries().size();
                connector.ack(batchId); // 提交确认
            }
        }catch (Exception e ){

        }
    }

    private void toES(List<CanalEntry.Entry> entrys) {
        for (CanalEntry.Entry entry : entrys) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            CanalEntry.RowChange rowChage = null;
            try {
                rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                    e);
            }

            CanalEntry.EventType eventType = rowChage.getEventType();
            System.out.println(String.format("================&gt; binlog[%s:%s] , name[%s,%s] , eventType : %s",
                entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                entry.getHeader().getSchemaName(), entry.getHeader().getTableName(), eventType));

            for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == CanalEntry.EventType.DELETE) {
                    // List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
                    // System.out.println(rowData.getAfterColumns(0).);
                    // printColumn(rowData.getBeforeColumnsList());
                    esService.deleteToIndex(rowData.getBeforeColumnsList());
                } else if (eventType == CanalEntry.EventType.INSERT) {
                    esService.saveToIndex(rowData.getAfterColumnsList());
                    // printColumn(rowData.getAfterColumnsList());
                } else {
                    // System.out.println("-------&gt; before");
                    // printColumn(rowData.getBeforeColumnsList());
                    // System.out.println("-------&gt; after");
                    // printColumn(rowData.getAfterColumnsList());
                    esService.updateToIndex(rowData.getAfterColumnsList());
                }
            }
        }
    }
}
