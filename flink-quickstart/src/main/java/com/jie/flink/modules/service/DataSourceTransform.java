package com.jie.flink.modules.service;

import com.jie.flink.modules.models.Student;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;

public class DataSourceTransform {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        initEnv(env);

        //获取数据: 监听本地9000端口
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9000);

        // Transform DataStream
        // 1. Transform Number
/*
        dataStreamSource.flatMap((FlatMapFunction<String, String>) (s, collector) -> { // flatMap: 按空格分割字符串
            for(String word : s.split(" ")) {
                collector.collect(word);
            }
        }).returns(Types.STRING
        ).map((MapFunction<String, Integer>) s -> Integer.parseInt(s) * 9             // map: 字符串转数字并 * 9
        ).filter((FilterFunction<Integer>) value -> value % 2 == 0                    // filter: 过滤所有奇数
        ).print();
*/

        // 2. Transform Object
/*
        dataStreamSource.flatMap((FlatMapFunction<String, Student>) (s, collector) -> {
            for(String student : s.split(" ")) {
                int id = Integer.parseInt(student.split(":")[0]);
                String name = student.split(":")[1];
                collector.collect(new Student(id, name, name, (int) Math.round(Math.random() * 100 +1)));
            }
        }).returns(Student.class)
        .filter((FilterFunction<Student>) student -> student.getId() % 2 == 0)
        .keyBy(Student::getId)
        .reduce((ReduceFunction<Student>) (student, t1) -> { // 归并操作: 求相同ID的学生年龄平均值
            Student student1 = new Student();
            student1.setId((student.getId() + t1.getId()) / 2);
            student1.setName(student.getName() + t1.getName());
            student1.setPassword(student.getPassword() + t1.getPassword());
            student1.setAge((student.getAge() + t1.getAge()) / 2);
            return student1;
        }).print();
*/

        // 3. Aggregations

        // 4. window
        dataStreamSource.map((MapFunction<String, Tuple2<String, Integer>>) s -> Tuple2.of(s, (int) Math.round(Math.random() * 100 +1))
        ).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(0
//        ).timeWindow(Time.seconds(10) // 滚动(不重合)窗口: tumbling time window 每10s统计一次
//        ).timeWindow(Time.seconds(10), Time.seconds(30) // 滑动窗口: sliding time window 每10s统计过去30s的数据
//        ).countWindow(10 // 统计10的元素
        ).reduce((ReduceFunction<Tuple2<String, Integer>>) (t1, t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1) // 等价于sum(1)
        ).print();

        env.execute("DataStream Transform Test");
    }

    protected static void initEnv(StreamExecutionEnvironment env) {
//        env.enableCheckpointing(30 * 1000); // 设置 checkpoint 的间隔和模式(默认EXACTLY_ONCE精确一次)
//        env.getCheckpointConfig()
//                .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    }

}
