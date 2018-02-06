package com.java.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Created by sgr on 2018/2/6.
 */
public class TakeSampleOperator {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("TakeSampleOperator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<String> list = Arrays.asList("1","2","3","4","5");
        JavaRDD<String> javaRDD = sparkContext.parallelize(list);

        //takeSample = take + sample
        //第一个参数：取样是否放回
        //第二个参数：取样的个数
        //第三个参数：种子，如果写死每次取样结果相同
        List<String> result = javaRDD.takeSample(false, 2, 100L);

        for(String s : result){
            System.out.println(s);
        }
        sparkContext.close();
    }
}
