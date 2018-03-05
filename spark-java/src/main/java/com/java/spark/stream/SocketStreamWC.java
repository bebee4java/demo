package com.java.spark.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * socket数据：
 * yum install nc
 * nc -lk 8888
 * Created by sgr on 2018/3/4/004.
 */
public class SocketStreamWC {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("SocketStreamWC").setMaster("local[4]");
        //创建JavaStreamingContext
        //第二个参数是指每收集多长时间的数据划分为一个Batch即RDD去执行，Durations可设置毫秒 秒分
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(3));
        //这里创建一个socket监听来持续不断的接收实时的数据流，RDD里的每一个元素就是一行文本
        //此时要求资源并行度即线程数要>=2，因为有一个线程会独占一直工作取数据
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("192.168.16.192",8888);
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        JavaPairDStream<String,Integer> wc = words.mapToPair(new PairFunction<String, String, Integer>() {
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
        //每次计算完成，打印一下统计情况
        wc.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
