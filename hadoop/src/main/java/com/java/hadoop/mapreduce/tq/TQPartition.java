package com.java.hadoop.mapreduce.tq;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * Created by sgr on 2017/10/18/018.
 */
public class TQPartition extends HashPartitioner<WeatherRecord,DoubleWritable> {
    /**
     * Use {@link Object#hashCode()} to partition.
     *
     * @param key WR 温度记录
     * @param value WD 温度
     * @param numReduceTasks reduce个数
     */

    //重写规则：1.满足业务 2.简单

    @Override
    public int getPartition(WeatherRecord key, DoubleWritable value, int numReduceTasks) {
        //分区规则：当前年份 - 1949 于 reduce个数取模
        return (key.getYear() - 1949) % numReduceTasks;

        //return super.getPartition(key, value, numReduceTasks);
    }
}
