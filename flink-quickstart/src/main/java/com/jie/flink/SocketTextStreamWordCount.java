package com.jie.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class SocketTextStreamWordCount {
    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //获取数据
        DataStreamSource<String> stream = env.socketTextStream("localhost", 9000);

        //计数
        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream  = stream.flatMap(new LineSplitter())
                .keyBy(0)
                .timeWindow(Time.seconds(15))
                .sum(1);

        dataStream.print();

        env.execute("Window WordCount");
    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> out) {
            for (String word : s.split(" ")) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }

}
