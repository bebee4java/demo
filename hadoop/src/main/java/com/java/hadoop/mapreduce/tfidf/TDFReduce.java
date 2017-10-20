package com.java.hadoop.mapreduce.tfidf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by sgr on 2017/10/20/020.
 */
public class TDFReduce extends Reducer<Text,IntWritable,Text,IntWritable> {

    /**
     * 统计包含某个词语的文档数目
     * @param key
     * @param values
     * @param context
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : values){
            sum += i.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
