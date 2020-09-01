package com.jie.flink.modules.service.window;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class WindowFunctionDemo {

	/**
	 * processWindowFunction
	 * @param dataStreamSource 数据源
	 */
	private static void processFunction(DataStreamSource<String> dataStreamSource) {
		dataStreamSource.map(s -> Tuple2.of(s, (int) Math.round(Math.random() * 100 + 1))).returns(Types.TUPLE(Types.STRING, Types.INT))
		        .keyBy(0)
				.timeWindow(Time.seconds(10))
		        .process(new ProcessWindowFunction<Tuple2<String, Integer>, String, Tuple, TimeWindow>() {
			        @Override
			        public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out)
			                throws Exception {
				        AtomicInteger sum = new AtomicInteger();
				        elements.forEach(element -> {
					        sum.addAndGet(element.f1);
				        });
				        out.collect(tuple.getField(0) + " --> sum: " + sum.get());
			        }
		        }).print();
	}

	/**
	 * processWindowFunction & ReduceFunction
	 * @param dataStreamSource 数据源
	 */
	private static void processFunctionWithReduce(DataStreamSource<String> dataStreamSource) {
		dataStreamSource.map(s -> Tuple2.of(s, (int) Math.round(Math.random() * 100 + 1))).returns(Types.TUPLE(Types.STRING, Types.INT))
				.keyBy(0)
				.timeWindow(Time.seconds(10))
				.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
						return t1.f1 > t2.f1 ? t1 : t2;
					}
				}, new ProcessWindowFunction<Tuple2<String, Integer>, Object, Tuple, TimeWindow>() {
					@Override
					public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Object> out) {
						Tuple2<String, Integer> element = elements.iterator().next();
						out.collect(new Tuple2<>(context.window().getStart(), element.f1));
					}
				})
				.print();
	}

	/**
	 * windowFunction
	 * @param dataStreamSource 数据源
	 */
	private static void applyFunction(DataStreamSource<String> dataStreamSource) {
		dataStreamSource.map(s -> Tuple2.of(s, (int) Math.round(Math.random() * 100 + 1))).returns(Types.TUPLE(Types.STRING, Types.INT))
				.keyBy(0)
				.timeWindow(Time.seconds(10))
				.apply(new WindowFunction<Tuple2<String, Integer>, Object, Tuple, TimeWindow>() {
					@Override
					public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Object> out) throws Exception {
						AtomicReference<Long> sum = new AtomicReference<>(0L);
						input.forEach(element -> {
							sum.updateAndGet(v -> v + element.f1);
						});
						out.collect("sum: " + sum.get());
					}
				})
				.print();
	}

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		//	获取数据: 监听本地9000端口 nc -l -p 9000
		DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9000);

//		processFunction(dataStreamSource);

//		processFunctionWithReduce(dataStreamSource);

		applyFunction(dataStreamSource);

		env.execute("Window Function Test");

	}

}
