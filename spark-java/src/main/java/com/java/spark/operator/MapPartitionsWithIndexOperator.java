package com.java.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by sgr on 2018/1/21/021.
 */
public class MapPartitionsWithIndexOperator {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("MapPartitionsWithIndexOperator").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        List<String> list = Arrays.asList("zhangsan", "lisi", "wangwu");
        JavaRDD<String> javaRDD = sparkContext.parallelize(list);

        //mapPartitionWithIndex可以拿个每个partition的index
        JavaRDD<String> result = javaRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
                List<String> names = new ArrayList<String>();
                while (iterator.hasNext()){
                    String name = iterator.next();
                    String s = index + ":" + name;
                    names.add(s);
                }
                return names.iterator();
            }
        },true);

        result.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sparkContext.close();
    }
}
