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
public class FilesystemConnectionSinkTest {

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
        configuration.setString("execution.checkpointing.interval", "30s");
        configuration.setString("table.exec.source.idle-timeout", "100ms");
//        configuration.setString("table.exec.resource.default-parallelism", "1");
//        configuration.setString("pipeline.auto-watermark-interval", "1s");

        // 批读(1.13不支持流读)
        // Streaming Sink 流写
        tableEnv.executeSql("create table kafka_dmaintenance_target_config (\n" +
                "    target_identity string,\n" +
                "    business_type string,\n" +
                "    ts BIGINT,\n" +
                "    ts_ltz AS TO_TIMESTAMP_LTZ(ts, 3),\n" +
//                "    ts_ltz TIMESTAMP(3),\n" +
                "    WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '5' SECOND\n" +
                ")  with (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'dmaintenance_target_config',\n" +
                "  'properties.bootstrap.servers' = '10.165.76.45:19092,10.165.76.46:19092,10.165.76.47:19092', \n" +
                "  'properties.group.id' = 'dmaintenance_target_config_group',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

//        // 非分区表
//        tableEnv.executeSql("CREATE TABLE dmaintenance_target_config (\n" +
//                "    target_identity string,\n" +
//                "    business_type string\n" +
//                ") WITH (\n" +
//                "  'connector' = 'filesystem',\n" +
//                "  'path' = 'F:\\test\\orc',\n" +
//                "  'format' = 'orc'\n" +
////                "  'sink.rolling-policy.rollover-interval' = '1min',\n" +
////                "  'sink.rolling-policy.check-interval' = '15s',\n" +
////                "  'sink.partition-commit.trigger'='process-time\t',\n" +
////                "  'sink.partition-commit.delay'='1min'\n" +
////                "  'auto-compaction' = 'false'\n" +
//                ")");

        // 分区表
        tableEnv.executeSql("CREATE TABLE dmaintenance_target_config_p (\n" +
                "    target_identity string,\n" +
                "    business_type string,\n" +
                "    dt STRING,\n" +
                "    `hour` STRING,\n" +
                "    `minute` STRING\n" +
                ") PARTITIONED BY (dt, `hour`, `minute`) WITH (\n" +
                "  'connector' = 'filesystem',\n" +
                "  'path' = 'F:\\test\\parquet',\n" +
                "  'format' = 'parquet',\n" +
                "  'partition.time-extractor.timestamp-pattern'='$dt $hour:$minute:00',\n" +
//                "  'partition.time-extractor.kind'='default',\n" +
                "  'sink.partition-commit.delay'='1min',\n" +
                "  'sink.partition-commit.trigger'='partition-time',\n" +
//                "  'sink.partition-commit.watermark-time-zone'='Asia/Shanghai',\n" +
                "  'sink.partition-commit.watermark-time-zone'='UTC',\n" +
//                "  'sink.rolling-policy.rollover-interval' = '15s',\n" +
//                "  'sink.rolling-policy.check-interval' = '5s',\n" +
                "  'sink.partition-commit.policy.kind'='success-file'\n" +
                ")");

        // 非分区表
//        tableEnv.executeSql("insert into dmaintenance_target_config " +
//                "select target_identity, business_type from kafka_dmaintenance_target_config");

        // 分区表
        tableEnv.executeSql("insert into dmaintenance_target_config_p " +
                "select target_identity, business_type, DATE_FORMAT(ts_ltz, 'yyyy-MM-dd'), DATE_FORMAT(ts_ltz, 'HH'), DATE_FORMAT(ts_ltz, 'mm') from kafka_dmaintenance_target_config");
    }

}