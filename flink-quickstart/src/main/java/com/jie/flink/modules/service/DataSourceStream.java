package com.jie.flink.modules.service;

import com.google.common.collect.Lists;
import com.jie.flink.source.MySQLSourceMaker;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.ArrayList;
import java.util.Properties;

public class DataSourceStream {

    public static void basicDataSource(StreamExecutionEnvironment env) {
        // Data Source from Collection.class: 有界数据集, 偏向本地测试
        ArrayList<String> list = Lists.newArrayList(
                "hello", "my", "name", "is", "leo"
        );

        env.fromCollection(list).print();

        // Data Source from File: 适合监听文件修改并读取其内容
        env.readTextFile("D:\\CommonUnits\\flink-1.10.0\\conf\\flink-conf.yaml").print();

        // Data Source from socket: 监听 localhost 的 9000 端口过来的数据
        env.socketTextStream("localhost", 9000);

        // customized source: 继实现SourceFunction接口
        env.addSource(new SourceFunction<Object>() {

            private static final long serialVersionUID = 1L;

            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<Object> sourceContext) {
                while(isRunning) {
                    sourceContext.collect(Math.floor(Math.random() * 100));
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }

        }).print();
    }

    public static void kafkaDataSource(StreamExecutionEnvironment env) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.0.30:9092");
        props.put("zookeeper.connect", "192.168.0.30:2181");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  //key 反序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest"); //value 反序列化

        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer011<>(
                "metric",  //kafka topic
                new SimpleStringSchema(),  // String 序列化
                props)).setParallelism(1);

        dataStreamSource.print(); //把从 kafka 读取到的数据打印在控制台
    }

    public static void mySQLDataSource(StreamExecutionEnvironment env) {
        env.addSource(new MySQLSourceMaker()).print();
    }

    public static void main(String[] args) throws Exception {

        // create execution environment: 根据上下文环境创建
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        basicDataSource(env);

        kafkaDataSource(env);

        mySQLDataSource(env);

        // execute job
        env.execute("Flink add data source");
    }

}

