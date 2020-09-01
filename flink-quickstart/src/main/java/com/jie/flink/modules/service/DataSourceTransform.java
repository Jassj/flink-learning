package com.jie.flink.modules.service;

import com.jie.flink.modules.models.Student;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class DataSourceTransform {

	// basic operators to transform number
	public static void transformNumber(DataStreamSource<String> dataStreamSource) {
		dataStreamSource.flatMap((FlatMapFunction<String, String>) (s, collector) -> {
			for (String word : s.split(" ")) {
				collector.collect(word);
			}
		}).returns(Types.STRING).map((MapFunction<String, Integer>) s -> Integer.parseInt(s) * 9).filter(value -> value % 2 == 0).print();
	}

	// transform dataStream to Object
	public static void transformObject(DataStreamSource<String> dataStreamSource) {
		dataStreamSource.flatMap((FlatMapFunction<String, Student>) (s, collector) -> {
			for (String student : s.split(" ")) {
				int id = Integer.parseInt(student.split(":")[0]);
				String name = student.split(":")[1];
				collector.collect(new Student(id, name, name, (int) Math.round(Math.random() * 100 + 1)));
			}
		}).returns(Student.class).filter(student -> student.getId() % 2 == 0).keyBy(Student::getId)
		        .reduce((student, t1) -> new Student(student.getId(), student.getName() + "-" + t1.getName(),
		                student.getPassword() + "-" + t1.getPassword(), (student.getAge() + t1.getAge()) / 2))
		        .print();
	}

	public static void aggregationOperators(DataStreamSource<String> dataStreamSource) {
		dataStreamSource.flatMap((FlatMapFunction<String, Tuple3<String, Integer, Integer>>) (s, collector) -> {
			for (String subStr : s.split(" ")) {
				int i = (int) Math.floor(Math.random() * 100 + 1);
				collector.collect(Tuple3.of(subStr, i, i));
			}
		}).returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT)).keyBy(0).maxBy(2).print();
	}

	// window operators: time window & count window
	public static void windowOperators(DataStreamSource<String> dataStreamSource) {
		dataStreamSource.map(s -> Tuple2.of(s, (int) Math.round(Math.random() * 100 + 1))).returns(Types.TUPLE(Types.STRING, Types.INT))
		        .keyBy(0)
				.timeWindow(Time.seconds(10))
		        .allowedLateness(Time.seconds(1))
		        .reduce((t1, t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1))
				.print();
	}

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		//		获取数据: 监听本地9000端口 nc -l -p 9000
		DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9000);

		//		transformNumber(dataStreamSource);

		//		transformObject(dataStreamSource);

		//		aggregationOperators(dataStreamSource);

		windowOperators(dataStreamSource);

		env.execute("DataStream Transform Test");

	}

}
