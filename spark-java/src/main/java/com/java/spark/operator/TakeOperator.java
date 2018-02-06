package com.java.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Created by sgr on 2018/2/6.
 */
public class TakeOperator {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("TakeOperator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<String> list = Arrays.asList("1","2","3","4","5");
        JavaRDD<String> javaRDD = sparkContext.parallelize(list);
        //action操作：去RDD的前n的数，慎用
        List<String> result = javaRDD.take(3);

        for (String s : result){
            System.out.println(s);
        }
        sparkContext.close();
    }
}
