package com.jie.flink;

import com.jie.flink.models.Student;
import com.jie.flink.sources.SourceFromMySQL;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

public class DataSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Data Source from MySQL
        SingleOutputStreamOperator<Student> studentDataStreamSource = env.addSource(new SourceFromMySQL());

        studentDataStreamSource.addSink(new PrintSinkFunction<>()); // 等价于studentDataStreamSource.print()

        // execute job
        env.execute("Flink add data source");
    }

}
