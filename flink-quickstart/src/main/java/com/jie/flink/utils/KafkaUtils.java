package com.jie.flink.utils;

import com.alibaba.fastjson.JSON;
import com.jie.flink.modules.models.Metric;
import com.jie.flink.modules.models.Student;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * description: produce and consume message to kafka
 * @author yuanjie 2020/05/17 23:25
 */
public class KafkaUtils {

    private static final String broker_list = "192.168.0.30:9092";
    private static final String topic = "metric";  // kafka topic, Flink 程序中需要统一
    private static final Properties props = new Properties() {
        {
            put("bootstrap.servers", broker_list);
            put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // key 序列化
            put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // value 序列化
        }
    };

    public static void writeToKafka() {
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        Metric metric = new Metric();
        metric.setTimestamp(System.currentTimeMillis());
        metric.setName("mem");

        Map<String, Object> fields = new HashMap<>();
        fields.put("used_percent", 90d);
        fields.put("max", 27244873d);
        fields.put("used", 17244873d);
        fields.put("init", 27244873d);

        metric.setFields(fields);

        ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, JSON.toJSONString(metric));
        producer.send(record);
        System.out.println("发送数据: " + JSON.toJSONString(metric));

        producer.flush();
    }

    public static void writeToKafkaLimited() {
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        for (int i = 1; i <= 100; i++) {
            Student student = new Student(i, "lemon" + i, "password" + i, 1 + i);
            ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, JSON.toJSONString(student));
            producer.send(record);
            System.out.println("发送数据: " + JSON.toJSONString(student));
        }

        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            Thread.sleep(1000);
            writeToKafka();
        }
//        writeToKafkaLimited();
    }
}
