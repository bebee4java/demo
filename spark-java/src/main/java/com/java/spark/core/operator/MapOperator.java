package com.java.spark.core.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * Created by sgr on 2018/1/20/020.
 */
public class MapOperator {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("MapOperator").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        List<Integer> numbers = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> javaRDD = sparkContext.parallelize(numbers);

        //map算子可对每个元素进行操作
        JavaRDD<Integer> result = javaRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer number) throws Exception {
                System.out.println("exec call...");
                return number * 10;
            }
        });

        result.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer number) throws Exception {
                System.out.println(number);//在集群worker上执行
            }
        });

        sparkContext.close();
    }
}
