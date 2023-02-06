package kd.dc.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * hive连接器测试
 *
 * @author jie2.yuan
 * @version 1.0.0
 * @since 2022/11/9
 */
public class HiveConnectionFormatTest {

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
        configuration.setString("table.sql-dialect", "hive");
        configuration.setString("execution.checkpointing.interval", "30s");

        tableEnv.executeSql("CREATE CATALOG myhive WITH (\n" +
                "    'type' = 'hive',\n" +
                "    'default-database' = 'default_database'\n" +
                ")");

        tableEnv.executeSql("USE CATALOG myhive");

//        TableResult result = tableEnv.executeSql("select name from users");
//        result.print();
//        TableResult result = tableEnv.executeSql("select name from cities/*+ OPTIONS('streaming-source.enable'='true', 'streaming-source.partition.include'='all') */");
//        result.print();
//        TableResult result = tableEnv.executeSql("select virtual_table_name from myhive.ods_govern.ods_sjzl_sjdt_table_info_ss");
//        result.print();

        // 文件格式测试 textfile
//        tableEnv.executeSql("CREATE TABLE if not exists users_text (\n" +
//                "  target_identity STRING,\n" +
//                "  business_type STRING\n" +
//                ") STORED AS textfile");
//
//        // 文件格式测试 orc
//        tableEnv.executeSql("CREATE TABLE if not exists users_orc (\n" +
//                "  target_identity STRING,\n" +
//                "  business_type STRING\n" +
//                ") STORED AS rcfile");
//
//        // 文件格式测试 sequencefile
//        tableEnv.executeSql("CREATE TABLE if not exists users_sequence (\n" +
//                "  target_identity STRING,\n" +
//                "  business_type STRING\n" +
//                ") STORED AS sequencefile");
//
//        // 文件格式测试 parquet
//        tableEnv.executeSql("CREATE TABLE if not exists users_parquet (\n" +
//                "  target_identity STRING,\n" +
//                "  business_type STRING\n" +
//                ") STORED AS parquet");
//
        configuration.setString("table.sql-dialect", "default");
//        tableEnv.executeSql("INSERT INTO users_orc \n" +
//                "SELECT target_identity, business_type FROM kafka_dmaintenance_target_config");
//        tableEnv.executeSql("INSERT INTO users_sequence \n" +
//                "SELECT target_identity, business_type FROM kafka_dmaintenance_target_config");
        tableEnv.executeSql("INSERT INTO users_parquet \n" +
                "SELECT target_identity, business_type FROM kafka_dmaintenance_target_config");
//        tableEnv.executeSql("INSERT INTO users_text \n" +
//                "SELECT target_identity, business_type FROM kafka_dmaintenance_target_config");
    }

}