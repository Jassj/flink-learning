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
public class HiveConnectionSinkTest {

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

        // Flink 支持批和流两种模式往 Hive 中写入数据，当作为批程序，只有当作业完成时，Flink 写入 Hive 表的数据才能被看见。批模式写入支持追加到现有的表或者覆盖现有的表。
        // 创建非分区表并写入数据
//        tableEnv.executeSql("create table if not exists `users` (id int, name string)");
//        tableEnv.executeSql("INSERT INTO `users` SELECT 2, 'tom'");
//        tableEnv.executeSql("INSERT OVERWRITE `users` SELECT 1, 'leo'");

        // 创建分区表并写入数据
//        tableEnv.executeSql("create table if not exists `users_p` (id int, name string) partitioned by (create_day string)");
//        // 向静态分区表写入数据
//        tableEnv.executeSql("INSERT INTO `users_p` PARTITION (create_day='2022-11-15') SELECT 1, 'tom'");
//        // 向动态分区表写入数据
//        tableEnv.executeSql("INSERT INTO `users_p` SELECT 2, 'tom', '2022-11-16'");

        // 流写会不断的往 Hive 中添加新数据，提交记录使它们可见。用户可以通过几个属性控制如何触发提交。流写不支持 Insert overwrite 。
        // 流写: 不支持 insert overwrite -- processtime
//        tableEnv.executeSql("CREATE TABLE if not exists users_p_s_t (\n" +
//                "  target_identity STRING,\n" +
//                "  business_type STRING\n" +
//                ") PARTITIONED BY (dt STRING) STORED AS parquet TBLPROPERTIES (\n" +
//                "  'sink.partition-commit.trigger'='process-time',\n" +
//                "  'sink.partition-commit.delay'='0s',\n" +
//                "  'sink.partition-commit.policy.kind'='metastore,success-file',\n" +
//                "  'sink.rolling-policy.rollover-interval'='30s'\n" +
//                ")");

//        configuration.setString("table.sql-dialect", "default");
//        tableEnv.executeSql("INSERT INTO users_p_s_t \n" +
//                "SELECT target_identity, business_type, DATE_FORMAT(NOW(), 'yyyy-MM-dd')\n" +
//                "FROM kafka_dmaintenance_target_config");

        // -- event time
        // 分区表
//        tableEnv.executeSql("CREATE TABLE if not exists dmaintenance_target_config_p (\n" +
//                "    target_identity string,\n" +
//                "    business_type string\n" +
//                ") PARTITIONED BY (dt STRING, `hour` STRING, `minute` STRING) STORED AS parquet TBLPROPERTIES (\n" +
//                "  'partition.time-extractor.timestamp-pattern'='$dt $hour:$minute:00',\n" +
//                "  'sink.partition-commit.delay'='1min',\n" +
//                "  'sink.partition-commit.trigger'='partition-time',\n" +
//                "  'sink.partition-commit.watermark-time-zone'='Asia/Shanghai',\n" +
//                "  'sink.partition-commit.policy.kind'='success-file'\n" +
//                ")");
//
        configuration.setString("table.sql-dialect", "default");
//        tableEnv.executeSql("create table if not exists kafka_dmaintenance_target_config_watermark (\n" +
//                "    target_identity string,\n" +
//                "    business_type string,\n" +
//                "    ts BIGINT,\n" +
//                "    ts_ltz AS TO_TIMESTAMP_LTZ(ts, 3),\n" +
//                "    WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '5' SECOND\n" +
//                ")  with (\n" +
//                "  'connector' = 'kafka',\n" +
//                "  'topic' = 'dmaintenance_target_config',\n" +
//                "  'properties.bootstrap.servers' = '10.165.76.45:19092,10.165.76.46:19092,10.165.76.47:19092', \n" +
//                "  'properties.group.id' = 'dmaintenance_target_config_group',\n" +
//                "  'scan.startup.mode' = 'latest-offset',\n" +
//                "  'format' = 'json'\n" +
//                ")");
//
        tableEnv.executeSql("insert into dmaintenance_target_config_p " +
                "select target_identity, business_type, DATE_FORMAT(ts_ltz, 'yyyy-MM-dd'), DATE_FORMAT(ts_ltz, 'HH'), DATE_FORMAT(ts_ltz, 'mm') from kafka_dmaintenance_target_config_watermark");
    }

}