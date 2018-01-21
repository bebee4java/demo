package com.java.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * Created by sgr on 2018/1/21/021.
 */
public class FilterOperator {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("FilterOperator").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        List<Integer> list = Arrays.asList(11,3,4,8,2,9,10);
        JavaRDD<Integer> javaRDD = sparkContext.parallelize(list);

        //filter算子：过滤。逻辑条件为true则保留下来，false就过滤掉
        JavaRDD<Integer> result = javaRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer num) throws Exception {
                return num % 2 == 0;
            }
        });

        result.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer num) throws Exception {
                System.out.println(num);
            }
        });

        sparkContext.close();
    }
}
