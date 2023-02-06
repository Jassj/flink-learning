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
public class FilesystemConnectionSourceTest {

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
        configuration.setString("execution.checkpointing.interval", "1min");

        // 批读(1.13不支持流读)
        tableEnv.executeSql("CREATE TABLE dmaintenance_target_config_p (\n" +
                "    target_identity string,\n" +
                "    business_type string,\n" +
                "    dt STRING,\n" +
                "    `hour` STRING,\n" +
                "    `minute` STRING\n" +
                ") PARTITIONED BY (dt, `hour`, `minute`) WITH (\n" +
                "  'connector' = 'filesystem',\n" +
                "  'path' = 'F:\\test\\json',\n" +
                "  'format' = 'json'\n" +
                ")");

        tableEnv.executeSql("CREATE TABLE print_table (\n" +
                "    target_identity string,\n" +
                "    business_type string\n" +
                ") WITH (\n" +
                " 'connector' = 'print'\n" +
                ")");

        tableEnv.executeSql("insert into print_table select target_identity, business_type from dmaintenance_target_config_p " +
                "where dt = '2022-11-28' and `hour` = '16' and `minute` = '56' ");
    }

}