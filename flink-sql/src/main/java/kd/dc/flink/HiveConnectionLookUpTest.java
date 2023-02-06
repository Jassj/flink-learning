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
public class HiveConnectionLookUpTest {

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
        configuration.setString("execution.checkpointing.interval", "30s");

        tableEnv.executeSql("CREATE CATALOG myhive WITH (\n" +
                "    'type' = 'hive',\n" +
                "    'default-database' = 'default_database'\n" +
                ")");

        tableEnv.executeSql("USE CATALOG myhive");

        tableEnv.executeSql("create table if not exists kafka_dmaintenance_target_config_look_up_test (\n" +
                "    target_identity string,\n" +
                "    proctime as PROCTIME()\n" +
                ")  with (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'dmaintenance_target_config',\n" +
                "  'properties.bootstrap.servers' = '10.165.76.45:19092,10.165.76.46:19092,10.165.76.47:19092', \n" +
                "  'properties.group.id' = 'dmaintenance_target_config_group',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

        tableEnv.executeSql("CREATE TABLE if not exists print_table (\n" +
                "    target_identity string,\n" +
                "    business_type string\n" +
                ") WITH (\n" +
                "   'connector' = 'print'\n" +
                ")");

        tableEnv.executeSql("insert into print_table " +
                "SELECT t.target_identity, dim.business_type FROM kafka_dmaintenance_target_config_look_up_test AS t \n" +
                "JOIN users_parquet/*+ OPTIONS('streaming-source.enable'='false', 'streaming-source.partition.include'='all', 'streaming-source.partition-order'='create-time', 'lookup.join.cache.ttl'='1min') */ " +
                "FOR SYSTEM_TIME AS OF t.proctime AS dim\n" +
                "ON t.target_identity = dim.target_identity");

    }

}