import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.Arrays;

/**
 * hive连接器测试
 *
 * @author jie2.yuan
 * @version 1.0.0
 * @since 2022/11/9
 */
public class HiveCatalogTest {

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "hive");
        System.setProperty("sun.security.krb5.debug", "true");
        System.setProperty("sun.security.spnego.debug", "true");
        System.setProperty("java.security.krb5.conf", "F:\\etc\\kafka\\krb5.conf");

        // 创建执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner() // 使用BlinkPlanner
//                .inBatchMode() // Batch模式，默认为StreamingMode
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.executeSql("CREATE CATALOG myhive WITH (\n" +
                "    'type' = 'hive',\n" +
                "    'default-database' = 'default_database'\n" +
//                "    'hive-conf-dir' = '/opt/hive-conf'\n" +
                ")");

        tableEnv.executeSql("USE CATALOG myhive");

        TableResult result = tableEnv.executeSql("SHOW TABLES");
        result.print();

        System.out.println(Arrays.toString(tableEnv.listCatalogs()));

//        tableEnv.executeSql("create table kafka_dmaintenance_target_config (\n" +
//                "    target_identity string,\n" +
//                "    business_type string\n" +
//                ")  with (\n" +
//                "  'connector' = 'kafka',\n" +
//                "  'topic' = 'dmaintenance_target_config',\n" +
//                "  'properties.bootstrap.servers' = '10.165.76.45:19092,10.165.76.46:19092,10.165.76.47:19092', \n" +
//                "  'properties.group.id' = 'dmaintenance_target_config_group',\n" +
//                "  'format' = 'json'\n" +
//                ")");

//        tableEnv.executeSql("create table if not exists es_dmaintenance_target_config (\n" +
//                "    target_identity string,\n" +
//                "    business_type string\n" +
//                ")  with (\n" +
//                "  'connector' = 'kd-elasticsearch6',\n" +
//                "  'hosts' = 'http://10.165.141.92:29200',\n" +
//                "  'index' = 'dmaintenance_target_config_test',\n" +
//                "  'kerberos.principal' = 'kafka/master01@KAFKA.COM',\n" +
//                "  'kerberos.user-keytab' = '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',\n" +
//                "  'kerberos.krb5-conf' = 'IyBDb25maWd1cmF0aW9uIHNuaXBwZXRzIG1heSBiZSBwbGFjZWQgaW4gdGhpcyBkaXJlY3RvcnkgYXMgd2VsbAppbmNsdWRlZGlyIC9ldGMva3JiNS5jb25mLmQvCgpbbG9nZ2luZ10KIGRlZmF1bHQgPSBGSUxFOi92YXIvbG9nL2tyYjVsaWJzLmxvZwoga2RjID0gRklMRTovdmFyL2xvZy9rcmI1a2RjLmxvZwogYWRtaW5fc2VydmVyID0gRklMRTovdmFyL2xvZy9rYWRtaW5kLmxvZwoKW2xpYmRlZmF1bHRzXQogZG5zX2xvb2t1cF9yZWFsbSA9IGZhbHNlCiB0aWNrZXRfbGlmZXRpbWUgPSAyNDBoCiByZW5ld19saWZldGltZSA9IDdkCiBmb3J3YXJkYWJsZSA9IHRydWUKIHJkbnMgPSBmYWxzZQogcGtpbml0X2FuY2hvcnMgPSBGSUxFOi9ldGMvcGtpL3Rscy9jZXJ0cy9jYS1idW5kbGUuY3J0CiBkZWZhdWx0X3JlYWxtID0gS0FGS0EuQ09NCiBkZWZhdWx0X2NjYWNoZV9uYW1lID0gS0VZUklORzpwZXJzaXN0ZW50OiV7dWlkfQoKW3JlYWxtc10KIEtBRktBLkNPTSA9IHsKICAga2RjID0gMTAuMTY1LjE0MS45MgogICBhZG1pbl9zZXJ2ZXIgPSAxMC4xNjUuMTQxLjkyCiB9CgpbZG9tYWluX3JlYWxtXQojIC5leGFtcGxlLmNvbSA9IEVYQU1QTEUuQ09NCiMgZXhhbXBsZS5jb20gPSBFWEFNUExFLkNPTQo=',\n" +
//                "  'kerberos.log-debug' = 'true',\n" +
//                "  'document-type' = '_doc'\n" +
//                ")");

        tableEnv.executeSql("insert into es_dmaintenance_target_config\n" +
                "   select target_identity,business_type from kafka_dmaintenance_target_config");

    }

}