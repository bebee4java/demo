package com.java.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by sgr on 2017/12/13.
 */
public class SparkTest_Lines {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkTest_Lines");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD rdd = sc.textFile("data_160w.csv");
        long count = rdd.count();
        System.out.println(count);
        sc.close();
    }
}
