package com.jie.flink;

import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;

public class DataSourceStream {

    public static void main(String[] args) throws Exception {

        // create execution environment: 根据上下文环境创建
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

        // execute job
        env.execute("Flink add data source");
    }

}

