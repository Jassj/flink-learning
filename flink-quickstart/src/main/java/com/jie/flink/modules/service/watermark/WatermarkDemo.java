package com.jie.flink.modules.service.watermark;

import lombok.Builder;
import lombok.Data;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * Flink时间特性与窗口
 *
 * @author jie2.yuan
 * @version 1.0.0
 * @since 2022/11/2
 */
public class WatermarkDemo {

    @Data
    @Builder
    static class MyEvent {
        private String name;
        private Long randomTimestamp;
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置每30s生成一次水位线
        env.getConfig().setAutoWatermarkInterval(5000);

        // 定义水位线策略
        WatermarkStrategy<MyEvent> watermarkStrategy = WatermarkStrategy.forGenerator(ctx -> new MyBoundedOutOfOrdernessWatermarks<MyEvent>(Duration.ofMillis(10)))
                .withIdleness(Duration.ofSeconds(30));

        // 提取事件时间
        WatermarkStrategy<MyEvent> watermarkStrategyWithTimestamp = watermarkStrategy
                .withTimestampAssigner(new SerializableTimestampAssigner<MyEvent>() {
                    @Override
                    public long extractTimestamp(MyEvent element, long recordTimestamp) {
                        System.out.printf("element timestamp [%s]", element.getRandomTimestamp());
                        System.out.println();
                        return element.getRandomTimestamp();
                    }
                });

        // 获取数据
        DataStreamSource<String> sourceStream = env.socketTextStream("localhost", 9000);

        // map操作
        sourceStream.map(s -> {
            long random = Math.round(Math.random() * 100 + 100);
            MyEvent event = MyEvent.builder().name(s).randomTimestamp(random).build();
            System.out.println("event --> " + event.toString());
            return event;
        }).assignTimestampsAndWatermarks(watermarkStrategyWithTimestamp);

        // 运行程序
        env.execute();
    }

}
