package com.panda.flink.sink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
/**
 * FlinkToHive Example
 * @author muxiaohui
 */
public class FlinkToHive {
    public static void main(String[] args) throws Exception {
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
                StreamExecutionEnvironment.getExecutionEnvironment(),
                EnvironmentSettings.newInstance().inStreamingMode().build()
        );

        // 配置Hive Catalog连接
        String catalogName = "hive_catalog";
        String hiveConfDir = "/etc/hive/conf";
        tableEnv.executeSql("CREATE CATALOG " + catalogName + " WITH ("
                + "'type' = 'hive', "
                + "'hive-conf-dir' = '" + hiveConfDir + "'"
                + ")");

        tableEnv.useCatalog(catalogName);
        tableEnv.useDatabase("target_db");

        // 将转换后的数据写入Hive表
        Table transformedTable = tableEnv.sqlQuery("SELECT orderId, totalAmount FROM orders_transformed");
        tableEnv.executeSql("INSERT INTO target_table SELECT * FROM " + transformedTable);
    }
}
