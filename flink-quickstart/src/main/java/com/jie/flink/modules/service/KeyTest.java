package com.jie.flink.modules.service;

import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Objects;

public class KeyTest {

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class Student {
        public String name;
        public Long age;
    }

    public static void main(String[] args) throws Exception {
        // create execution environment: 根据上下文环境创建
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Data Source from Collection.class: 有界数据集, 偏向本地测试
        ArrayList<String> list = Lists.newArrayList(
                "Leo", "Amy", "Jack", "Nancy", "Mike", "Leo"
        );

        DataStream<String> dataStream = env.fromCollection(list);

        SingleOutputStreamOperator<Student> studentStream = dataStream.map(str -> new Student(str, Math.round(Math.random() * 20)));

        studentStream.keyBy(new KeySelector<Student, String>() {
            @Override
            public String getKey(Student student) throws Exception {
                return student.getName();
            }
        }).process(new KeyedProcessFunction<String, Student, Student>() {

            private transient ValueState<String> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                StateTtlConfig ttlConfig = StateTtlConfig
                        .newBuilder(Time.seconds(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        .build();

                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("TestValueState", String.class);
                stateDescriptor.enableTimeToLive(ttlConfig);

                state = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void processElement(Student student, Context context, Collector<Student> collector) throws Exception {
                System.out.println("key: " + context.getCurrentKey() + ", state -->" + state.value());
                if (Objects.isNull(state.value())) {
                    state.update(student.getName());
                }
            }

        });

        env.execute();

    }

}