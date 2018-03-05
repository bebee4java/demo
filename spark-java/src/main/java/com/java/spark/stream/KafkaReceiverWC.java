package com.java.spark.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by sgr on 2018/3/4/004.
 */
public class KafkaReceiverWC {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("KafkaReceiverWC").setMaster("local[4]");
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
        Map<String,Integer> kafkaParams = new HashMap<String, Integer>();
        //key：topic名称
        //value:线程数，即开几个线程取receive数据
        kafkaParams.put("topic_1",1);
        //zookeeper地址
        String zkList = "192.168.16.192:2181,192.168.16.113:2181,192.168.16.145:2181";

        JavaPairReceiverInputDStream<String,String> lines = KafkaUtils.createStream(jsc,zkList,"KafkaReceiverWC",kafkaParams);
        JavaPairDStream<String,Integer> wc = lines.flatMap(new FlatMapFunction<Tuple2<String,String>, String>() {
            //tuple._1:kafka内部的偏移量
            //tuple._2:kafka具体的数据
            @Override
            public Iterator<String> call(Tuple2<String, String> tuple2) throws Exception {
                return Arrays.asList(tuple2._2.split(" ")).iterator();
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        wc.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.stop();
        jsc.close();
    }
}
