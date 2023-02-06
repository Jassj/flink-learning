package com.jie.flink.modules.service.window;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.CloseableIterable;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

public class WindowAggregateFunction {

    public static void main(String[] args) throws Exception {
        // create execution environment: 根据上下文环境创建
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

//        DataStreamSource<Double> streamSource = env.addSource(new SourceFunction<Double>() {
//
//            private static final long serialVersionUID = 1L;
//
//            private volatile boolean isRunning = true;
//
//            @Override
//            public void run(SourceContext<Double> sourceContext) throws InterruptedException {
//                while(isRunning) {
//                    sourceContext.collect(Math.floor(Math.random() * 100));
//                    Thread.sleep(1000);
//                }
//            }
//
//            @Override
//            public void cancel() {
//                isRunning = false;
//            }
//
//        });

        DataStreamSource<Double> streamSource = env.fromCollection(Lists.newArrayList(
                1.0, 2.0, 4.0, 11.0, 12.0
        ));

        streamSource.process(new ProcessFunction<Double, Object>() {
            @Override
            public void processElement(Double value, ProcessFunction<Double, Object>.Context ctx, Collector<Object> out) throws Exception {
//                ctx.timerService().registerEventTimeTimer();
            }

            @Override
            public void onTimer(long timestamp, ProcessFunction<Double, Object>.OnTimerContext ctx, Collector<Object> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
            }

        });

        SingleOutputStreamOperator<Double> aggregateStream = streamSource.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<Double, Double, Double>() {

                    @Override
                    public Double createAccumulator() {
                        return 0.0;
                    }

                    @Override
                    public Double add(Double value, Double accumulator) {
                        System.out.println("value ----" + value);
                        return value + accumulator;
                    }

                    @Override
                    public Double getResult(Double accumulator) {
                        System.out.println("accumulator ----" + accumulator);
                        return accumulator;
                    }

                    @Override
                    public Double merge(Double a, Double b) {
                        return null;
                    }
                });

//        aggregateStream.executeAndCollect().forEachRemaining(r -> System.out.println("result --- " + r));

        double sumResult = 0;
        try (CloseableIterator<Double> iterator = aggregateStream.executeAndCollect();) {
            while (iterator.hasNext()) {
                double result = iterator.next();
                sumResult += result;
                System.out.println(" iterator ---" + result);
            }
        }

        System.out.println("sumResult --- " + sumResult);
//        env.execute();

    }

}
