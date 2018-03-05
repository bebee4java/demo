package com.java.spark.stream;


import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * Created by sgr on 2018/3/5/005.
 */
public class KafkaDirectWC {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("KafkaDirectWC").setMaster("local[1]");
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        Map<String,String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list","192.168.16.192:9092,192.168.16.113:9092,192.168.16.145:9092");
        Set<String> topics = new HashSet<String>();
        topics.add("topic_1");
        JavaPairInputDStream<String,String> lines = KafkaUtils.createDirectStream(jsc,String.class, String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics);
        JavaPairDStream<String,Integer> wc = lines.flatMap(new FlatMapFunction<Tuple2<String,String>, String>() {
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
