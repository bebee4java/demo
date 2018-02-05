package com.java.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * Created by sgr on 2018/2/5.
 */
public class SampleOperator {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("AggregateByKeyOperator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<String> list = Arrays.asList("zhangsan", "lisi", "wangwu", "lilei","hanmeimei");
        JavaRDD<String> javaRDD = sparkContext.parallelize(list);
        //采样取数 第一个参数：取出的元素是否放回 第二个参数：抽取比例 第三个参数：种子 如果写死每次抽样相同
        javaRDD.sample(false,0.7).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        sparkContext.close();
    }
}
