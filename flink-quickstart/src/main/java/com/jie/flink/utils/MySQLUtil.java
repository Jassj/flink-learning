package com.jie.flink.utils;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Desc: MySQL 工具类
 */
public class MySQLUtil {

    public static Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");

            con = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/test?useUnicode=true&characterEncoding=UTF-8",
                    "root",
                    "072595");
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = "+ e.getMessage());
        }
        return con;
    }
}
