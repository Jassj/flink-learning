package kd.dc.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 * hive连接器测试
 *
 * @author jie2.yuan
 * @version 1.0.0
 * @since 2022/11/9
 */
public class HiveConnectionSourceTest {

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "hive");

        // 创建执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner() // 使用BlinkPlanner
                .inStreamingMode()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.dynamic-table-options.enabled", "true");
//        configuration.setString("table.sql-dialect", "hive");
        configuration.setString("execution.checkpointing.interval", "30s");

        tableEnv.executeSql("CREATE CATALOG myhive WITH (\n" +
                "    'type' = 'hive',\n" +
                "    'default-database' = 'default_database'\n" +
                ")");

        tableEnv.executeSql("USE CATALOG myhive");


        tableEnv.executeSql("CREATE TABLE if not exists print_table (\n" +
                "    target_identity string,\n" +
                "    business_type string\n" +
                ") WITH (\n" +
                "   'connector' = 'print'\n" +
                ")");

        // 读取不同格式的表
//        TableResult result = tableEnv.executeSql("select * from users_text");
//        result.print();
//        result = tableEnv.executeSql("select * from users_sequence");
//        result.print();
//        result = tableEnv.executeSql("select * from users_orc");
//        result.print();
//        result = tableEnv.executeSql("select * from users_parquet");
//        result.print();

        // SQL-HINT 流式读取非分区表
//        tableEnv.executeSql("insert into print_table select target_identity, business_type from users_text" +
//                "/*+ OPTIONS('streaming-source.enable'='true', 'streaming-source.partition.include'='all', 'streaming-source.partition-order'='create-time') */");

        // 流式读取分区表
        tableEnv.executeSql("insert into print_table select target_identity, business_type from dmaintenance_target_config_p" +
                "/*+ OPTIONS('streaming-source.enable'='true', 'streaming-source.partition.include'='all', 'streaming-source.partition-order'='partition-name', 'streaming-source.monitor-interval'='30s') */");
    }

}