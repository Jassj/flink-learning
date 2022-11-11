package kd.dc.flink;

import java.util.Arrays;

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
public class HiveConnectionTest {

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "hive");
//        System.setProperty("sun.security.krb5.debug", "true");
//        System.setProperty("sun.security.spnego.debug", "true");
//        System.setProperty("java.security.krb5.conf", "F:\\etc\\kafka\\krb5.conf");

        // 创建执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
//                .useBlinkPlanner() // 使用BlinkPlanner
//                .inStreamingMode()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.executeSql("CREATE CATALOG myhive WITH (\n" +
                "    'type' = 'hive',\n" +
                "    'default-database' = 'default'\n" +
//                "    'hive-conf-dir' = '/opt/hive-conf'\n" +
                ")");

        tableEnv.executeSql("USE CATALOG myhive");

//        tableEnv.executeSql("CREATE TABLE cities(\n" +
//                "  `name` string\n" +
//                ") with (\n" +
//                "  'streaming-source.enable' = 'false',\n" +
//                "  'streaming-source.partition.include' = 'all',\n" +
//                "  'lookup.join.cache.ttl' = '12 h'\n" +
//                ")");

        System.out.println(Arrays.toString(tableEnv.listCatalogs()));

        TableResult result = tableEnv.executeSql("select name from cities");
        result.print();


    }

}