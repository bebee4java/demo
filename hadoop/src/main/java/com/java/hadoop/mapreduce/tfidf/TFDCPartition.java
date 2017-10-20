package com.java.hadoop.mapreduce.tfidf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * 分区规则：最后一个区放语料库的文档数
 * Created by sgr on 2017/10/20/020.
 */
public class TFDCPartition extends HashPartitioner<Text,IntWritable> {

    /**
     * @param key
     * @param value
     * @param numReduceTasks
     */
    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
        String s = key.toString();
        if ("count".equals(s)){
            return 3;
        }
        return super.getPartition(key, value, numReduceTasks - 1 );
    }
}
