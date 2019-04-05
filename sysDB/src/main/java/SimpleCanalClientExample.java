import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import tools.KafkaUtils;


public class SimpleCanalClientExample {


    public static void main(String args[]) {
        // 创建链接
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("node1",
                11111), "example", "", "");
        int batchSize = 1000;
        int emptyCount = 0;
        try {
            connector.connect(); //开启连接
            connector.subscribe(".*\\..*"); //订阅消息
            connector.rollback(); //回滚
            int totalEmptyCount = 12000000;
            while (emptyCount < totalEmptyCount) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    //如果batchID== -1 或者 获取到的消息实体数量==0,认为本次没有获取到数据
                    emptyCount++;
                    System.out.println("empty count : " + emptyCount);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                } else {
//                    有数据发生了变更
                    emptyCount = 0;
                    // System.out.printf("message[batchId=%s,size=%s] \n", batchId, size);
                    //打印消息实体
                    printEntry(message.getEntries());
                }

                connector.ack(batchId); // 提交确认
                // connector.rollback(batchId); // 处理失败, 回滚数据
            }

            System.out.println("empty too many times, exit");
        } finally {
            connector.disconnect();
        }
    }

    private static void printEntry(List<Entry> entrys) {
        for (Entry entry : entrys) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }

            //rowChange是这一行的改变数据
            RowChange rowChage = null;
            try {
                rowChage = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            //获取本次消息改变的类型:增加/删除/修改
            EventType eventType = rowChage.getEventType();
            String logfileName = entry.getHeader().getLogfileName();
            long logfileOffset = entry.getHeader().getLogfileOffset();
            String dbName = entry.getHeader().getSchemaName();
            String tableName = entry.getHeader().getTableName();

//            mysql-bin.000001
//            3063
//            big_data
//            menu
//            UPDATE
//                    ==========================
//            id:12:false
//            name:测试新增:false
//            action::false
//            style:hello1:false
//            description::false
//            parent_id:2:true
//            deleted::false

//            mysql-bin.000001##*##3063##*##big_data##*##menu##*##UPDATE##*##[id:12:false,name:测试新增:false,action::false]
//          json
//          {
//              "logfileName":"mysql-bin.000001",
//              "logfileOffset":"23423",
//              "dbName":"big_data",
//                "rowData":[
//                            {"columnName": "id",
//                                "columnValue": "12",
//                                "update": "false"
//                            },
//                            {"columnName": "name",
//                                    "columnValue": "测试新增",
//                                    "update": "false"
//                            },{"columnName": "parent_id",
//                                    "columnValue": "2",
//                                    "update": "true"
//                            },
//
//
//                        ]
//          }

            System.out.println("==========================");
            System.out.println(logfileName);
            System.out.println(logfileOffset);
            System.out.println(dbName);
            System.out.println(tableName);
            System.out.println(eventType);
            System.out.println("==========================");

            for (RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == EventType.DELETE) {
//                    printColumn(rowData.getBeforeColumnsList());
                    sendKafka(logfileName, logfileOffset, dbName, tableName, eventType, rowData.getBeforeColumnsList());
                } else if (eventType == EventType.INSERT) {
//                    printColumn(rowData.getAfterColumnsList());
                    sendKafka(logfileName, logfileOffset, dbName, tableName, eventType, rowData.getAfterColumnsList());
                } else {
//                    printColumn(rowData.getAfterColumnsList());
                    sendKafka(logfileName, logfileOffset, dbName, tableName, eventType, rowData.getAfterColumnsList());
                }
            }
        }
    }

    /**
     * 将canal获取到的消息,封装为json对象,发送到Kafka
     * @param logfileName binlog日志文件名
     * @param logfileOffset binlog偏移量
     * @param dbName 数据库名字
     * @param tableName 表名字
     * @param eventType 事件发生的类型
     * @param columns 本条记录的所有内容
     */
    private static void sendKafka(String logfileName, long logfileOffset, String dbName, String tableName, EventType eventType, List<Column> columns) {
        HashMap<String, Object> map = new HashMap<String, Object>();
        map.put("logfileName", logfileName);
        map.put("logfileOffset", logfileOffset);
        map.put("dbName", dbName);
        map.put("tableName", tableName);
        map.put("eventType", eventType);

        //使用List集合封装所有的列信息
        ArrayList<Object> list = new ArrayList<Object>();

        //将每一列的数据变更信息,封装到map里面
        for (Column column : columns) {
            //创建一个Map,封装一列的信息
            HashMap<String, Object> cell = new HashMap<String, Object>();
            String columnName = column.getName();
            String columnValue = column.getValue();
            boolean columnUpdated = column.getUpdated();

            cell.put("columnName", columnName);
            cell.put("columnValue", columnValue);
            cell.put("columnUpdated", columnUpdated);
            //将该列的信息,放入List集合
            list.add(cell);
        }

        map.put("rowData", list);

        String data = JSON.toJSONString(map);

        System.out.println(data);

        KafkaUtils.send(data);

//        {"logfileOffset":3408,"dbName":"big_data","logfileName":"mysql-bin.000001","eventType":"UPDATE","tableName":"menu"}
//        {"logfileOffset":4150,
//        "dbName":"big_data",
//        "rowData":[
//        {"columnUpdated":false,"columnValue":"4","columnName":"id"},
//        {"columnUpdated":false,"columnValue":"收人分析","columnName":"name"},
//        {"columnUpdated":false,"columnValue":"","columnName":"action"},
//        {"columnUpdated":false,"columnValue":"mainicon_console_43","columnName":"style"},
//        {"columnUpdated":false,"columnValue":"","columnName":"description"},
//        {"columnUpdated":false,"columnValue":"2","columnName":"parent_id"},
//        {"columnUpdated":false,"columnValue":"0","columnName":"deleted"}],
//        "logfileName":"mysql-bin.000001",
//        "eventType":"DELETE",
//        "tableName":"menu"}

    }

    private static void printColumn(List<Column> columns) {
//        for (Column column : columns) {
//            String columnName = column.getName();
//            String columnValue = column.getValue();
//            boolean columnUpdated = column.getUpdated();
//            System.out.println(columnName + ":" + columnValue + ":" + columnUpdated);
//        }
    }
}