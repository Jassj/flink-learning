package com.jie.flink.modules.service;

import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class KeyTest {

//    @Data
    @AllArgsConstructor
//    @NoArgsConstructor
    static class Student {
        public String name;
        public Long age;
    }

    public static void main(String[] args) throws Exception {
        // create execution environment: 根据上下文环境创建
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Data Source from Collection.class: 有界数据集, 偏向本地测试
        ArrayList<String> list = Lists.newArrayList(
                "Leo", "Amy", "Jack", "Nancy", "Mike"
        );

		DataStream<String> dataStream = env.fromCollection(list);

		SingleOutputStreamOperator<Student> studentStream = dataStream.map(str -> new Student(str, Math.round(Math.random() * 20)));

        SingleOutputStreamOperator<Student> resultStream = studentStream.keyBy(new KeySelector<Student, Student>() {
            @Override
            public Student getKey(Student student) throws Exception {
                return student;
            }
        })
/*		SingleOutputStreamOperator<Student> resultStream = studentStream.keyBy(Student::getName)*/.process(new KeyedProcessFunction<Student, Student, Student>() {
            @Override
            public void processElement(Student student, Context context, Collector<Student> collector) throws Exception {
                System.out.println(context.getCurrentKey());
                System.out.println("-------------");
            }
        });

        env.execute();
            
    }

}