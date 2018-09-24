package com.java.flink.stram;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * flink流处理窗口时间函数，读取socket流
 *
 * @author sgr
 * @create 2018-09-24 01:29
 **/

public class StreamingWindowWC {
    public static void main(String[] args) throws Exception {
        int port;
        try {
            port = ParameterTool.fromArgs(args).getInt("port");
        } catch (Exception e) {
            port = 9000;
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", port, "\n");

        DataStream<Tuple2<String, Long>> wc = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] words = line.split("\\s");
                for (String word: words) {
                    collector.collect(new Tuple2<String, Long>(word, 1L));
                }
            }
        }).keyBy(0).timeWindow(Time.seconds(2), Time.seconds(1)).sum(1);

        wc.print().setParallelism(1);

        env.execute("flink streaming word count");


    }

}
