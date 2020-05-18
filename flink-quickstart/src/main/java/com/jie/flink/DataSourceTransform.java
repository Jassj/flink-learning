package com.jie.flink;

import com.jie.flink.models.Student;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataSourceTransform {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据: 监听本地9000端口
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9000);

        // Transform DataStream
        // 1. Transform Number
//        dataStreamSource.flatMap((FlatMapFunction<String, String>) (s, collector) -> { // flatMap: 按空格分割字符串
//            for(String word : s.split(" ")) {
//                collector.collect(word);
//            }
//        }).returns(Types.STRING
//        ).map((MapFunction<String, Integer>) s -> Integer.parseInt(s) * 9             // map: 字符串转数字并 * 9
//        ).filter((FilterFunction<Integer>) value -> value % 2 == 0                    // filter: 过滤所有奇数
//        ).print();

        // 2. Transform Object
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

        env.execute("DataStream Transform Test");
    }

}
