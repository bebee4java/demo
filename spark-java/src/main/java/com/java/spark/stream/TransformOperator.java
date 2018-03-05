package com.java.spark.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by sgr on 2018/3/4/004.
 */
public class TransformOperator {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("TransformOperator").setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(10));

        List<Tuple2<String, Boolean>> list = Arrays.asList(
                new Tuple2<String, Boolean>("zhangsan", true),
                new Tuple2<String, Boolean>("wangwu",false)
        );
        //黑名单
        final JavaPairRDD<String,Boolean> blackNames = jsc.sparkContext().parallelizePairs(list);
        //socket每行为 name string 模拟每个用户的消息
        //过滤黑名单里的用户（true）
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("192.168.16.192",8888);

        JavaDStream<String> result = lines.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String line) throws Exception {
                return new Tuple2<String, String>(line.split(" ")[0], line);
            }
        }).transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> rdd) throws Exception {
                JavaRDD<String> javaRDD = rdd.leftOuterJoin(blackNames).filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple2) throws Exception {
                        if (tuple2._2._2.isPresent() && tuple2._2._2.get()){
                            return false;
                        }
                        return true;
                    }
                }).map(new Function<Tuple2<String,Tuple2<String,Optional<Boolean>>>, String>() {
                    @Override
                    public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple2) throws Exception {
                        return tuple2._2._1;
                    }
                });
                return javaRDD;
            }
        });

        result.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.stop();
        jsc.close();

    }
}
