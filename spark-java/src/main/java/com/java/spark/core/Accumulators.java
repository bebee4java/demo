package com.java.spark.core;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.List;

/**
 * Created by sgr on 2018/2/19/019.
 */
public class Accumulators {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Accumulators");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        final LongAccumulator longAccumulator = sparkContext.sc().longAccumulator();

        final Accumulator<Integer> accumulator = sparkContext.accumulator(0, "Accumulators");
        List<Integer> list = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> javaRDD = sparkContext.parallelize(list);

        javaRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer v) throws Exception {
//                accumulator.add(v);
                longAccumulator.add(v);
            }
        });

//        System.out.println(accumulator.value());
        System.out.println(longAccumulator.value());

        try {
            Thread.sleep(60 * 60 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        sparkContext.close();
    }
}
