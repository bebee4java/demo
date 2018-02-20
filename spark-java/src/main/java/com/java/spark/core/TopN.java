package com.java.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

/**
 * Created by sgr on 2018/2/10.
 */
public class TopN {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("TopN");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<Integer> list = Arrays.asList(2,3,1,4,5,6,9,7,8);
        JavaRDD<Integer> javaRDD = sparkContext.parallelize(list);
        List<Integer> result =
        javaRDD.sortBy(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1;
            }
        }, false,1).take(3);

        for (Integer i : result){
            System.out.println(i);
        }
        sparkContext.close();
    }
}
