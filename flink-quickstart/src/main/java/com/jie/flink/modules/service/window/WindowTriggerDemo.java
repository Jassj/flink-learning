package com.jie.flink.modules.service.window;

import com.jie.flink.modules.service.trigger.TimeAndCountTrigger;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.atomic.AtomicReference;

public class WindowTriggerDemo {

	public static void main(String[] args) throws Exception {

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> stream = env.addSource(new SourceFunction<String>() {

			private static final long serialVersionUID = 1L;

			private volatile boolean isRunning = true;

			@Override
			public void run(SourceContext<String> sourceContext) throws InterruptedException {
				while(isRunning) {
					sourceContext.collect(String.valueOf(Math.round(Math.random() * 10)));
					Thread.sleep(10);
				}
			}

			@Override
			public void cancel() {
				isRunning = false;
			}

		});

		SingleOutputStreamOperator<String> resultStream = stream
		        .keyBy((KeySelector<String, Integer>) Integer::parseInt).window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
				.trigger(new TimeAndCountTrigger())
		        .apply(new WindowFunction<String, String, Integer, TimeWindow>() {
					@Override
					public void apply(Integer key, TimeWindow window, Iterable<String> iterable, Collector<String> collector) throws Exception {
						AtomicReference<Integer> count = new AtomicReference<>(0);
						iterable.forEach(value -> count.getAndSet(count.get() + 1));
						System.out.println(String.format("key: [%s], count[%s], winodw[%s]", key, count.get(), window.maxTimestamp()));
					}
				}).setParallelism(5);

		resultStream.print();

		env.execute("Window WordCount");
	}

}