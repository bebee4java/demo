package com.java.spark.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 相当于全局的wordCount
 * Created by sgr on 2018/3/4/004.
 */
public class UpdateStateByKeyWC {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("UpdateStateByKeyWC").setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        //必须设值，为了缓存state
        jsc.checkpoint(".");
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("192.168.16.192",8888);

        JavaPairDStream<String,Integer> wc = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        }).updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            /**
             * 实际上对于每个单词，每次batch计算的时候，都会调用这个函数第一个函数values相当于这个batch中
             * 这个key对应的一组新值（可能有多个），第二个参数state表示这个key之前的状态
             */
            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                Integer newValue = 0;
                if (state.isPresent()){
                    newValue = state.get();
                }
                for (Integer v : values){
                    newValue += v;
                }

                return Optional.of(newValue);
            }
        });

        wc.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();

    }
}
