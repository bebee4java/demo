package com.java.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Created by sgr on 2018/2/4.
 */
public class CountOperator {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("CountOperator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<String> list = Arrays.asList("1", "2", "3", "4");
        JavaRDD<String> javaRDD = sparkContext.parallelize(list);
        long count = javaRDD.count();
        System.out.println(count);
        sparkContext.close();
    }
}
