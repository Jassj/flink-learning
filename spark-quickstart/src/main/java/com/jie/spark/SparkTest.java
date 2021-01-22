package com.jie.spark;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

public class SparkTest {

    public static void main(String[] args) {

//        String inputPath = "D:\\test\\test.txt";
//        String outPutPath = "D:\\test\\result.txt";
        // 创建java Spark Context
        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 读取输入数据
//        JavaRDD<String> input = sc.textFile(inputPath);
        JavaRDD<String> input = sc.parallelize(Arrays.asList("1 2 2 3"));

        // 数据持久化
        input.cache();

        // 切分为单词
        JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
//                System.out.println(s);
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

//        System.out.println(words.count());
//        System.out.println(words.countByValue());

        JavaDoubleRDD doubles = words.mapToDouble(new DoubleFunction<String>() {
            @Override
            public double call(String s) throws Exception {
                return Double.parseDouble(s);
            }
        });

        for(Double doubleValue : doubles.collect()) {
            System.out.println(doubleValue);
        }
        
        // Pair RDD
        JavaPairRDD<String, Long> tuple2 = words.mapToPair(new PairFunction<String, String, Long>() {
            @Override
            public Tuple2<String, Long> call(String s) throws Exception {
                return new Tuple2<>(s, Math.round(Math.random() * 100 + 1));
            }
        });

        for(Tuple2<String, Long> tuple : tuple2.collect()) {
            tuple._1(); // 获取位置1的元素
            tuple._2(); // 获取位置2的元素
            System.out.println(tuple.toString());
        }



//        // 转换为键值对并计数
//        JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(String s) throws Exception {
//                return new Tuple2<>(s, 1);
//            }
//        }).reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum);

//        words.foreach(System.out::println);

//        for(String word : words.take(2)) {
//            System.out.println(word);
//        }
//
//        for(String word : words.collect()) {
//            System.out.println(word);
//        }

//        // 统计的单词总数输出到文本
//        counts.saveAsTextFile(outPutPath);

    }

}