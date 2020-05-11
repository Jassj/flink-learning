package com.jie.flink.utils;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Desc: MySQL 工具类
 */
public class MySQLUtil {

    public static Connection getConnection(String driver, String url, String user, String password) {
        Connection con = null;
        try {
            Class.forName(driver);
            //注意，这里替换成你自己的mysql 数据库路径和用户名、密码
            con = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = "+ e.getMessage());
        }
        return con;
    }
}
