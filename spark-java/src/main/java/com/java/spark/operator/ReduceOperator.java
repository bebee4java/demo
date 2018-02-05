package com.java.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by sgr on 2018/2/5.
 */
public class ReduceOperator {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("ReduceOperator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<Integer> list = Arrays.asList(1,3,4,5,6,7);
        JavaRDD<Integer> javaRDD = sparkContext.parallelize(list);

        /**
         * reduce算子操作：将第一个和第二个元素传入call方法计算出结果
         * 然后把结果和后面的元素以此传入call方法计算，以此类推
         */
        int sum = javaRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer num1, Integer num2) throws Exception {
                return num1 + num2;
            }
        });
        System.out.println(sum);
        sparkContext.close();
    }
}
