package com.java.spark.core.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * Created by sgr on 2018/2/6.
 */
public class UnionOperator {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("TakeOperator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<String> list1 = Arrays.asList("1","2","3","4","5");
        List<String> list2 = Arrays.asList("a","b","c","d");
        JavaRDD<String> javaRDD1 = sparkContext.parallelize(list1,2);
        JavaRDD<String> javaRDD2 = sparkContext.parallelize(list2,2);

        //union算子逻辑上的抽象，不会改变之前partition的个数
        javaRDD1.union(javaRDD2).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sparkContext.close();

    }
}
