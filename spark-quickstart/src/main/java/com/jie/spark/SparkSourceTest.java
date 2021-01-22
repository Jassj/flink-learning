package com.jie.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class SparkSourceTest {

    public static void main(String[] args) {

		SparkSession sparkSession = SparkSession.builder()/*.enableHiveSupport()*/
				.appName("MyApp").master("local").getOrCreate();

        // 创建java Spark Context
//        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        SQLContext sqlContext = new SQLContext(sc);

		Dataset<Row> jdbcDF = sparkSession.read()
				.format("jdbc")
				.option("url", "jdbc:mysql://10.100.156.103:3500/TCDCapiinfo")
				.option("dbtable", "project_trace")
				.option("user", "TCDCapiinfo_local")
				.option("password", "uqbxUwlRNkPFA3vSJwkrp")
				.load();

        System.out.println(jdbcDF.count());
    }

}