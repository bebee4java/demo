package com.java.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.*;

/**
 * Created by sgr on 2018/1/20/020.
 */
public class MapPartitionsOperator {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("MapPartitionsOperator").setMaster("local[2]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<String> names = Arrays.asList("zhangsan","lisi","wangwu");

        //rdd的操作通常在不同机器Executor上执行，所有在网络传输过程中这个实例不能被改动
        final Map<String,Integer> scores = new HashMap<String, Integer>();
        scores.put("zhangsan",90);
        scores.put("lisi",98);
        scores.put("wangwu",70);
        JavaRDD<String> javaRDD = sparkContext.parallelize(names);

        /**
         * map算子一次处理一个partition的一条数据，而mappartition算子一次处理一个partition的所有数据
         *
         * 使用场景：如果rdd的数据不是特别多的时候，采用mappartition算子代替map算子可以提高处理速度
         * 但是rdd数据量特别大的时候不建议使用mappartition算子，会造成内存溢出
         */

        JavaRDD<Integer> scoresRdd = javaRDD.mapPartitions(new FlatMapFunction<Iterator<String>, Integer>() {
            @Override
            public Iterator<Integer> call(Iterator<String> names) throws Exception {
                List<Integer> list = new ArrayList<Integer>();
                while (names.hasNext()){
                    String name = names.next();
                    Integer score = scores.get(name);
                    list.add(score);
                }
                return list.iterator();
            }
        });

        scoresRdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer score) throws Exception {
                System.out.println(score);
            }
        });

    }
}
