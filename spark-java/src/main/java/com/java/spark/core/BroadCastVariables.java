package com.java.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * Created by sgr on 2018/2/19/019.
 */
public class BroadCastVariables {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("BroadCastVariables");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        final int f = 3;
        final Broadcast<Integer> broadcast = sparkContext.broadcast(f);

        List<Integer> list = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> javaRDD = sparkContext.parallelize(list);
        javaRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v) throws Exception {
                //广播变量只读，能够减少复制，减少内存存储
                return v * broadcast.value();
//                return v * f;  每个task会拷贝f的值
            }
        }).foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer v) throws Exception {
                System.out.println(v);
            }
        });
        sparkContext.close();
    }
}
