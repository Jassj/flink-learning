package com.jie.flink.sources;

import com.jie.flink.models.Student;
import com.jie.flink.utils.MySQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Desc: 自定义 source，从 mysql 中读取数据
 */
public class SourceFromMySQL extends RichSourceFunction<Student> {

    PreparedStatement ps;
    private Connection connection;

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接。
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = MySQLUtil.getConnection("com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8",
                "root",
                "root123456");
        String sql = "select * from Student;";
        ps = this.connection.prepareStatement(sql);
    }

    /**
     * 程序执行完毕: 关闭连接, 释放资源
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    /**
     * DataStream 调用一次 run() 方法用来获取数据
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Student> ctx) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            Student student = new Student(
                    resultSet.getInt("id"),
                    resultSet.getString("name").trim(),
                    resultSet.getString("password").trim(),
                    resultSet.getInt("age"));
            ctx.collect(student);
        }
    }

    @Override
    public void cancel() {
    }
}
