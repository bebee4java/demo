package com.java.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * Created by sgr on 2018/2/6.
 */
public class IntersectionOperator {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("TakeOperator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<String> list1 = Arrays.asList("a","a","b","e","f");
        List<String> list2 = Arrays.asList("a","b","c","d");
        JavaRDD<String> javaRDD1 = sparkContext.parallelize(list1,2);
        JavaRDD<String> javaRDD2 = sparkContext.parallelize(list2,2);

        //取交集  附去重
        javaRDD1.intersection(javaRDD2).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }
}
