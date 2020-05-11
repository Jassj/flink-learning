package com.jie.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SocketTextStreamWordCount {
    public static void main(String[] args) throws Exception {
        //参数检查
        if (args.length != 2) {
            System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>");
            return;
        }

        String hostname = args[0];
        Integer port = Integer.parseInt(args[1]);


        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.addSource(null);
//        env.fromCollection(null); // 从 Java 的 Java.util.Collection 创建数据流。集合中的所有元素类型必须相同
//        env.fromCollection(null, Integer.class); // 从一个迭代器中创建数据流。Class 指定了该迭代器返回元素的类型。
//        env.fromElements(
//                new Event(1, "barfoo", 1.0),
//                new Event(2, "start", 2.0),
//                new Event(3, "foobar", 3.0)
//        );


        //获取数据
        DataStreamSource<String> stream = env.socketTextStream(hostname, port);

        //计数
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stream.flatMap(new LineSplitter())
                .keyBy(0)
                .sum(1);

        sum.print();

        env.execute("Java WordCount from SocketTextStream Example");
    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
            String[] tokens = s.toLowerCase().split("\\W+");

            for (String token: tokens) {
                if (token.length() > 0) {
                    collector.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }

}
