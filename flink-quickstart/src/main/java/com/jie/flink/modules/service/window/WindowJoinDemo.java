package com.jie.flink.modules.service.window;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowJoinDemo {

    private static void joinFunction(DataStreamSource<String> firstStream, DataStreamSource<String> secondStream) {
        SingleOutputStreamOperator<Tuple2<String, Integer>> newFirstStream = firstStream.map(s -> Tuple2.of(s, (int) Math.round(Math.random() * 100 + 1))).returns(Types.TUPLE(Types.STRING, Types.INT));
        SingleOutputStreamOperator<Tuple2<String, Integer>> newSecondStream = secondStream.map(s -> Tuple2.of(s, (int) Math.round(Math.random() * 100 + 1))).returns(Types.TUPLE(Types.STRING, Types.INT));
        newFirstStream.join(newSecondStream)
                .where(t -> t.f0)
                .equalTo(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply((JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Object>) (first, second) -> first.f1 + second.f1)
                .print();
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig();


        //	获取数据: 监听本地xxxx端口 z -l -p xxxx
        DataStreamSource<String> firstStream = env.socketTextStream("localhost", 9000);
        DataStreamSource<String> secondStream = env.socketTextStream("localhost", 9001);

        joinFunction(firstStream, secondStream);

        env.execute("Window Function Test");

    }

}