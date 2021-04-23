package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstants;
import com.atguigu.util.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        //1.获取客户端连接器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");

        while (true){
            //2.获取Canal连接
            canalConnector.connect();

            //3.指定监控的数据库
            canalConnector.subscribe("gmall.*");

            //4.获取数据->首先获取Message
            Message message = canalConnector.get(100);

            //5.解析Message获取Entry
            List<CanalEntry.Entry> entries = message.getEntries();

            //6.判断list集合中是否有entry->是否有数据
            if (entries.size()>0){

                //7.遍历存放entry的list集合，以获取具体的entry
                for (CanalEntry.Entry entry : entries) {
                    //TODO 8.获取表名
                    String tableName = entry.getHeader().getTableName();

                    //TODO 9.获取Entry的类型
                    CanalEntry.EntryType entryType = entry.getEntryType();

                    //10.判断Entry的类型应为ROWDATA->这里才是数据
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)){

                        //11.获取序列化前的数据
                        ByteString storeValue = entry.getStoreValue();

                        //TODO 12.对数据进行反序列化
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                        //TODO 13.获取数据类型如（更新，插入，删除，创建）
                        CanalEntry.EventType eventType = rowChange.getEventType();

                        //TODO 14.根据表名，数据类型（更新，插入，删除，创建），对及反序列化后的数据进行解析
                        handler(tableName, eventType, rowChange);

                    }
                }
            }else {
                System.out.println("没有数据，休息一会！！！！！");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }


        }
    }

    /**
     * 根据表名，数据类型（更新，插入，删除，创建），对及反序列化后的数据进行解析
     * @param tableName
     * @param eventType
     * @param rowChange
     */
    private static void handler(String tableName, CanalEntry.EventType eventType, CanalEntry.RowChange rowChange) {
        //1.根据表名，数据类型来判断需要的是什么数据
        if ("order_info".equals(tableName)&&CanalEntry.EventType.INSERT.equals(eventType)){
            //2.解析反序列后的数据
            sendToKafka(GmallConstants.KAFKA_TOPIC_ORDER,rowChange);
            //获取订单详情新增的数据
        }else if ("order_detail".equals(tableName)&&CanalEntry.EventType.INSERT.equals(eventType)){
            sendToKafka(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL,rowChange);
            //获取用户表的新增及变化数据
        }else if ("user_info".equals(tableName)&&(CanalEntry.EventType.INSERT.equals(eventType)||CanalEntry.EventType.UPDATE.equals(eventType))){
            sendToKafka(GmallConstants.KAFKA_TOPIC_USER,rowChange);
        }
    }

    private static void sendToKafka(String topic,CanalEntry.RowChange rowChange) {
        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

        for (CanalEntry.RowData rowData : rowDatasList) {
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {
                jsonObject.put(column.getName(), column.getValue());
            }
            System.out.println(jsonObject.toString());
            //模拟网络延迟的效果
//            try {
//                Thread.sleep(new Random().nextInt(5)*1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            MyKafkaSender.send(topic, jsonObject.toString());
        }
    }
}
