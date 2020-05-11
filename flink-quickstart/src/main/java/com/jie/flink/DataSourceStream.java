package com.jie.flink;

import com.google.common.collect.Lists;
import com.jie.flink.sources.SourceFromMySQL;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;

public class DataSourceStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Data Source from Collection.class: 有界数据集, 偏向本地测试
        ArrayList<String> list = Lists.newArrayList(
                "hello", "my", "name", "is", "leo"
        );
        env.fromCollection(list).print();
//        env.fromCollection(list.iterator(), String.class).print();

        // Data Source from File: 适合监听文件修改并读取其内容
        env.readTextFile("D:\\CommonUnits\\flink-1.10.0\\conf\\flink-conf.yaml").print();

        // Data Source from socket
//        env.socketTextStream("localhost", 9000) // 监听 localhost 的 9000 端口过来的数据
//                .flatMap(new Splitter())
//                .keyBy(0)
//                .timeWindow(Time.seconds(5))
//                .sum(1);

        // customized source: 继承并实现SourceFunction接口
        env.addSource(new SourceFunction<Object>() {

            private static final long serialVersionUID = 1L;

            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<Object> sourceContext) throws Exception {
                while(isRunning) {
                    sourceContext.collect(Math.floor(Math.random() * 100));
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }

        }).print();

        // Data Source from MySQL
        env.addSource(new SourceFromMySQL()).print();


        // execute job
        env.execute("Flink add data source");
    }

}

